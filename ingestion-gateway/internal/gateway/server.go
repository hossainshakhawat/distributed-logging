package gateway

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/hossainshakhawat/distributed-logging/store-kafka/kafka"
	"github.com/hossainshakhawat/distributed-logging/store-kafka/models"
)

// Config holds gateway settings.
type Config struct {
	ListenAddr   string
	RateLimitRPS int
	ValidAPIKeys map[string]string // tenantID -> apiKey
}

// Server is the HTTP ingestion server.
type Server struct {
	cfg      Config
	producer kafka.Producer
	limiter  *rateLimiter
	mux      *http.ServeMux
}

// NewServer constructs a Server and registers routes.
func NewServer(cfg Config, producer kafka.Producer) *Server {
	s := &Server{
		cfg:      cfg,
		producer: producer,
		limiter:  newRateLimiter(cfg.RateLimitRPS),
		mux:      http.NewServeMux(),
	}
	s.mux.HandleFunc("/ingest", s.handleIngest)
	s.mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// handleIngest authenticates, decompresses, validates and publishes log batches.
func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	tenantID := r.Header.Get("X-Tenant-ID")
	apiKey := r.Header.Get("X-API-Key")
	if !s.authenticate(tenantID, apiKey) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	if !s.limiter.allow(tenantID) {
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	body, err := s.readBody(r)
	if err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}

	var batch models.LogBatch
	if err := json.Unmarshal(body, &batch); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	now := time.Now().UTC()
	for i := range batch.Entries {
		batch.Entries[i].TenantID = tenantID // enforce server-side tenant
		batch.Entries[i].IngestTimestamp = now
	}

	msg := kafka.Message{
		Topic:     kafka.TopicLogsRaw,
		Key:       kafka.LogPartitionKey(tenantID, batch.Entries[0].Service),
		Timestamp: now,
	}
	if err := kafka.MarshalValue(&msg, batch); err != nil {
		log.Printf("gateway: marshal: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if err := s.producer.Publish(context.Background(), msg); err != nil {
		log.Printf("gateway: kafka publish: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) authenticate(tenantID, apiKey string) bool {
	if tenantID == "" {
		return false
	}
	// Allow agents without API key in dev mode (empty ValidAPIKeys map).
	if len(s.cfg.ValidAPIKeys) == 0 {
		return true
	}
	expected, ok := s.cfg.ValidAPIKeys[tenantID]
	return ok && expected == apiKey
}

func (s *Server) readBody(r *http.Request) ([]byte, error) {
	var reader io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			return nil, err
		}
		defer gz.Close()
		reader = gz
	}
	return io.ReadAll(io.LimitReader(reader, 10<<20)) // 10 MB limit
}

// rateLimiter is a simple per-tenant token bucket (approx).
type rateLimiter struct {
	rps     int
	mu      sync.Mutex
	buckets map[string]*bucket
}

type bucket struct {
	tokens   float64
	lastSeen time.Time
}

func newRateLimiter(rps int) *rateLimiter {
	return &rateLimiter{rps: rps, buckets: make(map[string]*bucket)}
}

func (rl *rateLimiter) allow(tenantID string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	now := time.Now()
	b, ok := rl.buckets[tenantID]
	if !ok {
		b = &bucket{tokens: float64(rl.rps), lastSeen: now}
		rl.buckets[tenantID] = b
	}
	elapsed := now.Sub(b.lastSeen).Seconds()
	b.tokens += elapsed * float64(rl.rps)
	if b.tokens > float64(rl.rps) {
		b.tokens = float64(rl.rps)
	}
	b.lastSeen = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

package tail

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/distributed-logging/shared/kafka"
	"github.com/distributed-logging/shared/models"
)

// Config holds tail service settings.
type Config struct {
	ListenAddr        string
	MaxActiveSessions int
}

// Server streams live logs over SSE.
type Server struct {
	cfg            Config
	consumer       kafka.Consumer
	mux            *http.ServeMux
	activeSessions atomic.Int64
	brokerMu       sync.RWMutex
	brokers        map[string][]chan models.LogEntry // tenantID -> channels
}

// NewServer constructs the Server and starts the fan-out loop.
func NewServer(cfg Config, consumer kafka.Consumer) *Server {
	s := &Server{
		cfg:      cfg,
		consumer: consumer,
		mux:      http.NewServeMux(),
		brokers:  make(map[string][]chan models.LogEntry),
	}
	s.mux.HandleFunc("/tail", s.handleTail)
	s.mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	go s.fanOut()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// handleTail opens an SSE stream filtered by tenant, service, and level.
func (s *Server) handleTail(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	tenantID := r.Header.Get("X-Tenant-ID")
	if tenantID == "" {
		http.Error(w, "missing tenant", http.StatusUnauthorized)
		return
	}

	if s.activeSessions.Load() >= int64(s.cfg.MaxActiveSessions) {
		http.Error(w, "too many active sessions", http.StatusServiceUnavailable)
		return
	}
	s.activeSessions.Add(1)
	defer s.activeSessions.Add(-1)

	service := r.URL.Query().Get("service")
	level := strings.ToUpper(r.URL.Query().Get("level"))

	ch := make(chan models.LogEntry, 64)
	s.register(tenantID, ch)
	defer s.deregister(tenantID, ch)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case entry, ok := <-ch:
			if !ok {
				return
			}
			if service != "" && entry.Service != service {
				continue
			}
			if level != "" && entry.Level != level {
				continue
			}
			b, _ := json.Marshal(entry)
			if _, err := w.Write(append([]byte("data: "), append(b, '\n', '\n')...)); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// fanOut consumes from Kafka and dispatches to registered channels.
func (s *Server) fanOut() {
	if err := s.consumer.Subscribe([]string{kafka.TopicLogsNormalized}); err != nil {
		log.Printf("tail: subscribe: %v", err)
		return
	}
	ctx := context.Background()
	for {
		msg, err := s.consumer.Poll(ctx)
		if err != nil {
			log.Printf("tail: poll: %v", err)
			continue
		}
		var entry models.LogEntry
		if err := kafka.UnmarshalValue(msg, &entry); err != nil {
			continue
		}
		s.dispatch(entry)
	}
}

func (s *Server) dispatch(entry models.LogEntry) {
	s.brokerMu.RLock()
	chans := s.brokers[entry.TenantID]
	s.brokerMu.RUnlock()
	for _, ch := range chans {
		select {
		case ch <- entry:
		default: // drop if consumer is slow
		}
	}
}

func (s *Server) register(tenantID string, ch chan models.LogEntry) {
	s.brokerMu.Lock()
	s.brokers[tenantID] = append(s.brokers[tenantID], ch)
	s.brokerMu.Unlock()
}

func (s *Server) deregister(tenantID string, ch chan models.LogEntry) {
	s.brokerMu.Lock()
	defer s.brokerMu.Unlock()
	chans := s.brokers[tenantID]
	for i, c := range chans {
		if c == ch {
			s.brokers[tenantID] = append(chans[:i], chans[i+1:]...)
			break
		}
	}
}

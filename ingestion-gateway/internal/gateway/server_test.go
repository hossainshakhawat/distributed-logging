package gateway

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/hossainshakhawat/distributed-logging/store-kafka/kafka"
	"github.com/hossainshakhawat/distributed-logging/store-kafka/models"
)

// ── mock producer ─────────────────────────────────────────────────────────────

type mockProducer struct {
	published []kafka.Message
}

func (m *mockProducer) Publish(_ context.Context, msg kafka.Message) error {
	m.published = append(m.published, msg)
	return nil
}
func (m *mockProducer) Close() error { return nil }

// ── helpers ───────────────────────────────────────────────────────────────────

func newDevServer(rps int) (*Server, *mockProducer) {
	prod := &mockProducer{}
	cfg := Config{RateLimitRPS: rps, RawTopic: kafka.TopicLogsRaw} // empty ValidAPIKeys = dev mode
	return NewServer(cfg, prod), prod
}

func validBatchJSON(tenantID, service string) []byte {
	batch := models.LogBatch{
		AgentID: "agent-1",
		Entries: []models.LogEntry{
			{LogID: "l1", TenantID: tenantID, Service: service, Level: "INFO", Message: "hello"},
		},
		SentAt: time.Now().UTC(),
	}
	b, _ := json.Marshal(batch)
	return b
}

func postIngest(srv http.Handler, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/ingest", bytes.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	return rr
}

// ── /healthz ─────────────────────────────────────────────────────────────────

func TestHealthz(t *testing.T) {
	srv, _ := newDevServer(100)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("healthz: got %d, want 200", rr.Code)
	}
}

// ── /ingest method check ──────────────────────────────────────────────────────

func TestIngest_wrongMethod(t *testing.T) {
	srv, _ := newDevServer(100)
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		req := httptest.NewRequest(method, "/ingest", nil)
		rr := httptest.NewRecorder()
		srv.ServeHTTP(rr, req)
		if rr.Code != http.StatusMethodNotAllowed {
			t.Errorf("method %s: got %d, want 405", method, rr.Code)
		}
	}
}

// ── authenticate ─────────────────────────────────────────────────────────────

func TestAuthenticate_devMode_alwaysTrue(t *testing.T) {
	s := &Server{cfg: Config{ValidAPIKeys: nil}}
	if !s.authenticate("any-tenant", "any-key") {
		t.Fatal("dev mode (empty ValidAPIKeys) should always authenticate")
	}
	if !s.authenticate("any-tenant", "") {
		t.Fatal("dev mode should work even with empty api key")
	}
}

func TestAuthenticate_devMode_emptyTenantFails(t *testing.T) {
	s := &Server{cfg: Config{ValidAPIKeys: nil}}
	if s.authenticate("", "any-key") {
		t.Fatal("empty tenantID should fail authentication")
	}
}

func TestAuthenticate_validKey(t *testing.T) {
	s := &Server{cfg: Config{ValidAPIKeys: map[string]string{"t1": "secret"}}}
	if !s.authenticate("t1", "secret") {
		t.Fatal("valid key should authenticate")
	}
}

func TestAuthenticate_wrongKey(t *testing.T) {
	s := &Server{cfg: Config{ValidAPIKeys: map[string]string{"t1": "secret"}}}
	if s.authenticate("t1", "wrong") {
		t.Fatal("wrong key should fail")
	}
}

func TestAuthenticate_unknownTenant(t *testing.T) {
	s := &Server{cfg: Config{ValidAPIKeys: map[string]string{"t1": "secret"}}}
	if s.authenticate("unknown", "secret") {
		t.Fatal("unknown tenant should fail")
	}
}

func TestAuthenticate_emptyTenantID(t *testing.T) {
	s := &Server{cfg: Config{ValidAPIKeys: map[string]string{"t1": "secret"}}}
	if s.authenticate("", "secret") {
		t.Fatal("empty tenantID always fails")
	}
}

// ── /ingest authentication checks ────────────────────────────────────────────

func TestIngest_missingTenantID_unauthorized(t *testing.T) {
	srv, _ := newDevServer(100)
	// Dev mode but missing X-Tenant-ID
	rr := postIngest(srv, validBatchJSON("", "svc"), map[string]string{})
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("missing tenant: got %d, want 401", rr.Code)
	}
}

func TestIngest_wrongAPIKey_unauthorized(t *testing.T) {
	prod := &mockProducer{}
	srv := NewServer(Config{
		RateLimitRPS: 100,
		RawTopic:     kafka.TopicLogsRaw,
		ValidAPIKeys: map[string]string{"t1": "correct-key"},
	}, prod)
	rr := postIngest(srv, validBatchJSON("t1", "svc"), map[string]string{
		"X-Tenant-ID": "t1",
		"X-API-Key":   "wrong-key",
	})
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("wrong key: got %d, want 401", rr.Code)
	}
}

// ── /ingest valid request ─────────────────────────────────────────────────────

func TestIngest_validDevMode_accepted(t *testing.T) {
	srv, prod := newDevServer(100)
	rr := postIngest(srv, validBatchJSON("tenant-a", "my-svc"), map[string]string{
		"X-Tenant-ID":  "tenant-a",
		"Content-Type": "application/json",
	})
	if rr.Code != http.StatusAccepted {
		t.Errorf("valid request: got %d, want 202", rr.Code)
	}
	if len(prod.published) != 1 {
		t.Errorf("expected 1 Kafka message, got %d", len(prod.published))
	}
	if prod.published[0].Topic != kafka.TopicLogsRaw {
		t.Errorf("expected topic %q, got %q", kafka.TopicLogsRaw, prod.published[0].Topic)
	}
}

func TestIngest_validWithAPIKey_accepted(t *testing.T) {
	prod := &mockProducer{}
	srv := NewServer(Config{
		RateLimitRPS: 100,
		RawTopic:     kafka.TopicLogsRaw,
		ValidAPIKeys: map[string]string{"t1": "my-key"},
	}, prod)
	rr := postIngest(srv, validBatchJSON("t1", "svc"), map[string]string{
		"X-Tenant-ID":  "t1",
		"X-API-Key":    "my-key",
		"Content-Type": "application/json",
	})
	if rr.Code != http.StatusAccepted {
		t.Errorf("valid+key: got %d, want 202", rr.Code)
	}
}

// ── /ingest gzip body ─────────────────────────────────────────────────────────

func TestIngest_gzipBody_accepted(t *testing.T) {
	srv, prod := newDevServer(100)

	raw := validBatchJSON("tenant-a", "svc")
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, _ = gz.Write(raw)
	_ = gz.Close()

	req := httptest.NewRequest(http.MethodPost, "/ingest", &buf)
	req.Header.Set("X-Tenant-ID", "tenant-a")
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Errorf("gzip body: got %d, want 202 (body: %s)", rr.Code, rr.Body.String())
	}
	if len(prod.published) != 1 {
		t.Errorf("expected 1 published message, got %d", len(prod.published))
	}
}

// ── /ingest invalid JSON ──────────────────────────────────────────────────────

func TestIngest_invalidJSON_badRequest(t *testing.T) {
	srv, _ := newDevServer(100)
	rr := postIngest(srv, []byte("not-json{{"), map[string]string{
		"X-Tenant-ID":  "t1",
		"Content-Type": "application/json",
	})
	if rr.Code != http.StatusBadRequest {
		t.Errorf("invalid json: got %d, want 400", rr.Code)
	}
}

// ── /ingest rate limiting ─────────────────────────────────────────────────────

func TestIngest_rateLimited(t *testing.T) {
	srv, _ := newDevServer(1) // 1 req/s
	headers := map[string]string{"X-Tenant-ID": "t1", "Content-Type": "application/json"}
	body := validBatchJSON("t1", "svc")

	// First call should succeed.
	r1 := postIngest(srv, body, headers)
	if r1.Code != http.StatusAccepted {
		t.Fatalf("first request: got %d, want 202", r1.Code)
	}
	// Second immediate call should be rate-limited.
	r2 := postIngest(srv, body, headers)
	if r2.Code != http.StatusTooManyRequests {
		t.Errorf("second request: got %d, want 429", r2.Code)
	}
}

// ── rateLimiter ───────────────────────────────────────────────────────────────

func TestRateLimiter_allow(t *testing.T) {
	rl := newRateLimiter(1)
	if !rl.allow("t1") {
		t.Fatal("first request should be allowed")
	}
	if rl.allow("t1") {
		t.Fatal("second immediate request should be denied")
	}
}

func TestRateLimiter_differentTenants(t *testing.T) {
	rl := newRateLimiter(1)
	if !rl.allow("t1") {
		t.Fatal("t1 first request should be allowed")
	}
	if !rl.allow("t2") {
		t.Fatal("t2 first request should be allowed (separate bucket)")
	}
}

func TestRateLimiter_highRPS_neverBlocked(t *testing.T) {
	rl := newRateLimiter(1000)
	for i := 0; i < 100; i++ {
		if !rl.allow("tenant") {
			t.Fatalf("high RPS should not block on call %d", i)
		}
		time.Sleep(time.Millisecond) // ensures token refill
	}
}

// ── readBody ──────────────────────────────────────────────────────────────────

func TestReadBody_plain(t *testing.T) {
	s := &Server{}
	body := []byte(`{"hello":"world"}`)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	got, err := s.readBody(req)
	if err != nil {
		t.Fatalf("readBody plain: %v", err)
	}
	if string(got) != string(body) {
		t.Errorf("readBody plain: got %q, want %q", got, body)
	}
}

func TestReadBody_gzip(t *testing.T) {
	s := &Server{}
	original := []byte(`{"compressed":"data"}`)
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, _ = gz.Write(original)
	_ = gz.Close()

	req := httptest.NewRequest(http.MethodPost, "/", &buf)
	req.Header.Set("Content-Encoding", "gzip")
	got, err := s.readBody(req)
	if err != nil {
		t.Fatalf("readBody gzip: %v", err)
	}
	if string(got) != string(original) {
		t.Errorf("readBody gzip: got %q, want %q", got, original)
	}
}

func TestReadBody_invalidGzip(t *testing.T) {
	s := &Server{}
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("not-gzip"))
	req.Header.Set("Content-Encoding", "gzip")
	_, err := s.readBody(req)
	if err == nil {
		t.Fatal("expected error for invalid gzip body")
	}
}

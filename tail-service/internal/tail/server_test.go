package tail

import (
	"bytes"
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

// ── mock consumer ─────────────────────────────────────────────────────────────

type mockConsumer struct {
	msgs chan *kafka.Message
}

func newMockConsumer() *mockConsumer {
	return &mockConsumer{msgs: make(chan *kafka.Message, 16)}
}

func (m *mockConsumer) Subscribe(_ []string) error { return nil }
func (m *mockConsumer) Poll(ctx context.Context) (*kafka.Message, error) {
	select {
	case msg := <-m.msgs:
		return msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (m *mockConsumer) Commit(_ context.Context, _ *kafka.Message) error { return nil }
func (m *mockConsumer) Close() error                                     { return nil }

func (m *mockConsumer) push(entry models.LogEntry) {
	msg := &kafka.Message{}
	_ = kafka.MarshalValue(msg, entry)
	m.msgs <- msg
}

// ── helpers ───────────────────────────────────────────────────────────────────

func newTestServer(maxSessions int) *Server {
	cfg := Config{ListenAddr: ":0", MaxActiveSessions: maxSessions}
	return NewServer(cfg, newMockConsumer())
}

// ── /healthz ─────────────────────────────────────────────────────────────────

func TestHealthz(t *testing.T) {
	srv := newTestServer(10)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("healthz: got %d, want 200", rr.Code)
	}
}

// ── handleTail – missing tenant ───────────────────────────────────────────────

func TestHandleTail_missingTenant_unauthorized(t *testing.T) {
	srv := newTestServer(10)
	req := httptest.NewRequest(http.MethodGet, "/tail", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("missing tenant: got %d, want 401", rr.Code)
	}
}

// ── handleTail – too many sessions ───────────────────────────────────────────

func TestHandleTail_tooManySessions(t *testing.T) {
	srv := newTestServer(0) // max=0 → always over limit
	req := httptest.NewRequest(http.MethodGet, "/tail", nil)
	req.Header.Set("X-Tenant-ID", "t1")
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("too many sessions: got %d, want 503", rr.Code)
	}
}

// ── handleTail – non-SSE client (no Flusher) ─────────────────────────────────

func TestHandleTail_noFlusher_internalError(t *testing.T) {
	srv := newTestServer(10)
	req := httptest.NewRequest(http.MethodGet, "/tail", nil)
	req.Header.Set("X-Tenant-ID", "t1")
	w := &noFlusherWriter{}
	srv.ServeHTTP(w, req)
	if w.code != http.StatusInternalServerError {
		t.Errorf("no flusher: got %d, want 500", w.code)
	}
}

// noFlusherWriter implements only http.ResponseWriter (no Flush method).
type noFlusherWriter struct {
	code    int
	headers http.Header
	body    bytes.Buffer
}

func (w *noFlusherWriter) Header() http.Header {
	if w.headers == nil {
		w.headers = make(http.Header)
	}
	return w.headers
}
func (w *noFlusherWriter) Write(b []byte) (int, error) { return w.body.Write(b) }
func (w *noFlusherWriter) WriteHeader(code int)        { w.code = code }

// ── register / deregister ─────────────────────────────────────────────────────

func TestRegister_addsChannel(t *testing.T) {
	srv := newTestServer(10)
	ch := make(chan models.LogEntry, 4)
	srv.register("t1", ch)

	srv.brokerMu.RLock()
	n := len(srv.brokers["t1"])
	srv.brokerMu.RUnlock()

	if n != 1 {
		t.Errorf("expected 1 channel registered, got %d", n)
	}
}

func TestDeregister_removesChannel(t *testing.T) {
	srv := newTestServer(10)
	ch := make(chan models.LogEntry, 4)
	srv.register("t1", ch)
	srv.deregister("t1", ch)

	srv.brokerMu.RLock()
	n := len(srv.brokers["t1"])
	srv.brokerMu.RUnlock()

	if n != 0 {
		t.Errorf("expected 0 channels after deregister, got %d", n)
	}
}

func TestDeregister_unknownChannel_noOp(t *testing.T) {
	srv := newTestServer(10)
	other := make(chan models.LogEntry, 4)
	ch := make(chan models.LogEntry, 4)
	srv.register("t1", other)
	// Deregister a channel that was never registered.
	srv.deregister("t1", ch)

	srv.brokerMu.RLock()
	n := len(srv.brokers["t1"])
	srv.brokerMu.RUnlock()

	if n != 1 {
		t.Errorf("deregistering unknown channel should be a no-op, got %d channels", n)
	}
}

func TestRegister_multipleTenants(t *testing.T) {
	srv := newTestServer(10)
	ch1 := make(chan models.LogEntry, 4)
	ch2 := make(chan models.LogEntry, 4)
	srv.register("t1", ch1)
	srv.register("t2", ch2)

	srv.brokerMu.RLock()
	n1 := len(srv.brokers["t1"])
	n2 := len(srv.brokers["t2"])
	srv.brokerMu.RUnlock()

	if n1 != 1 || n2 != 1 {
		t.Errorf("each tenant should have 1 channel: t1=%d t2=%d", n1, n2)
	}
}

// ── dispatch ──────────────────────────────────────────────────────────────────

func TestDispatch_sendsToRegisteredChannel(t *testing.T) {
	srv := newTestServer(10)
	ch := make(chan models.LogEntry, 4)
	srv.register("t1", ch)

	entry := models.LogEntry{TenantID: "t1", Service: "svc-a", Message: "hello"}
	srv.dispatch(entry)

	select {
	case got := <-ch:
		if got.Message != "hello" {
			t.Errorf("dispatched message: got %q, want %q", got.Message, "hello")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for dispatched entry")
	}
}

func TestDispatch_doesNotSendToOtherTenant(t *testing.T) {
	srv := newTestServer(10)
	ch := make(chan models.LogEntry, 4)
	srv.register("t2", ch) // register for t2

	// Dispatch for t1 — t2's channel should NOT receive anything.
	entry := models.LogEntry{TenantID: "t1", Message: "t1-only"}
	srv.dispatch(entry)

	select {
	case <-ch:
		t.Fatal("t2 channel should not receive t1's entry")
	case <-time.After(30 * time.Millisecond):
		// Expected: nothing received.
	}
}

func TestDispatch_dropsWhenChannelFull(t *testing.T) {
	srv := newTestServer(10)
	ch := make(chan models.LogEntry) // unbuffered — dispatch should drop
	srv.register("t1", ch)

	// Should not block.
	srv.dispatch(models.LogEntry{TenantID: "t1", Message: "drop-me"})
	// No assertion needed — test passes if it doesn't block.
}

func TestDispatch_multipleSubscribers(t *testing.T) {
	srv := newTestServer(10)
	ch1 := make(chan models.LogEntry, 4)
	ch2 := make(chan models.LogEntry, 4)
	srv.register("t1", ch1)
	srv.register("t1", ch2)

	entry := models.LogEntry{TenantID: "t1", Message: "broadcast"}
	srv.dispatch(entry)

	for i, ch := range []chan models.LogEntry{ch1, ch2} {
		select {
		case got := <-ch:
			if got.Message != "broadcast" {
				t.Errorf("subscriber %d: got %q", i, got.Message)
			}
		case <-time.After(100 * time.Millisecond):
			t.Errorf("subscriber %d: timed out waiting for entry", i)
		}
	}
}

// ── SSE streaming ─────────────────────────────────────────────────────────────

func TestHandleTail_streamsEntry(t *testing.T) {
	srv := newTestServer(10)

	// Use a context with cancellation to end the SSE stream.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/tail", nil).WithContext(ctx)
	req.Header.Set("X-Tenant-ID", "t1")

	rr := httptest.NewRecorder()

	// Run handler in a goroutine (it blocks on channel / context).
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.ServeHTTP(rr, req)
	}()

	// Give handler time to register its internal channel.
	time.Sleep(20 * time.Millisecond)

	// Dispatch via srv so the handler's registered channel receives the entry.
	entry := models.LogEntry{TenantID: "t1", Service: "svc", Level: "ERROR", Message: "streamed"}
	srv.dispatch(entry)

	time.Sleep(50 * time.Millisecond)
	cancel() // end the SSE stream
	<-done

	body := rr.Body.String()
	if !strings.Contains(body, "data:") {
		t.Errorf("SSE response should contain 'data:' prefix, got: %q", body)
	}
	if !strings.Contains(body, "streamed") {
		t.Errorf("SSE response should contain entry message, got: %q", body)
	}
}

func TestHandleTail_filtersService(t *testing.T) {
	srv := newTestServer(10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/tail?service=wanted-svc", nil).WithContext(ctx)
	req.Header.Set("X-Tenant-ID", "t1")
	rr := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.ServeHTTP(rr, req)
	}()

	// Give handler time to register its channel.
	time.Sleep(20 * time.Millisecond)

	// This entry has the wrong service — should be filtered.
	srv.dispatch(models.LogEntry{TenantID: "t1", Service: "other-svc", Message: "filtered-out"})
	// This entry matches.
	srv.dispatch(models.LogEntry{TenantID: "t1", Service: "wanted-svc", Message: "included"})

	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	body := rr.Body.String()
	if strings.Contains(body, "filtered-out") {
		t.Error("filtered-out entry should not appear in stream")
	}
	if !strings.Contains(body, "included") {
		t.Error("matching entry should appear in stream")
	}
}

func TestHandleTail_filtersLevel(t *testing.T) {
	srv := newTestServer(10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/tail?level=ERROR", nil).WithContext(ctx)
	req.Header.Set("X-Tenant-ID", "t1")
	rr := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.ServeHTTP(rr, req)
	}()

	// Give handler time to register its channel.
	time.Sleep(20 * time.Millisecond)

	srv.dispatch(models.LogEntry{TenantID: "t1", Level: "INFO", Message: "info-message"})
	srv.dispatch(models.LogEntry{TenantID: "t1", Level: "ERROR", Message: "error-message"})

	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	body := rr.Body.String()
	if strings.Contains(body, "info-message") {
		t.Error("INFO entry should be filtered when level=ERROR")
	}
	if !strings.Contains(body, "error-message") {
		t.Error("ERROR entry should pass the level filter")
	}
}

func TestHandleTail_setsSSEHeaders(t *testing.T) {
	srv := newTestServer(10)
	ctx, cancel := context.WithCancel(context.Background())

	req := httptest.NewRequest(http.MethodGet, "/tail", nil).WithContext(ctx)
	req.Header.Set("X-Tenant-ID", "t1")
	rr := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.ServeHTTP(rr, req)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	if ct := rr.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type: got %q, want text/event-stream", ct)
	}
	if cc := rr.Header().Get("Cache-Control"); cc != "no-cache" {
		t.Errorf("Cache-Control: got %q, want no-cache", cc)
	}
}

// ── activeSessions counter ────────────────────────────────────────────────────

func TestHandleTail_decrementsActiveSessionsOnExit(t *testing.T) {
	srv := newTestServer(10)

	if srv.activeSessions.Load() != 0 {
		t.Fatalf("initial activeSessions should be 0")
	}

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/tail", nil).WithContext(ctx)
	req.Header.Set("X-Tenant-ID", "t1")
	rr := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.ServeHTTP(rr, req)
	}()

	time.Sleep(20 * time.Millisecond)
	if srv.activeSessions.Load() != 1 {
		t.Errorf("activeSessions during handler: got %d, want 1", srv.activeSessions.Load())
	}

	cancel()
	<-done

	if srv.activeSessions.Load() != 0 {
		t.Errorf("activeSessions after handler exits: got %d, want 0", srv.activeSessions.Load())
	}
}

// ── fanOut integration ────────────────────────────────────────────────────────

func TestFanOut_dispatchesConsumedEntry(t *testing.T) {
	consumer := newMockConsumer()
	cfg := Config{ListenAddr: ":0", MaxActiveSessions: 10}
	srv := NewServer(cfg, consumer) // starts fanOut goroutine

	ch := make(chan models.LogEntry, 4)
	srv.register("tenant-x", ch)

	// Push a log entry into the mock consumer.
	consumer.push(models.LogEntry{TenantID: "tenant-x", Service: "svc", Message: "fan-out-test"})

	select {
	case got := <-ch:
		if got.Message != "fan-out-test" {
			t.Errorf("fanOut: got message %q, want %q", got.Message, "fan-out-test")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for fanOut to deliver entry")
	}
}

// ── NewServer wires routes ────────────────────────────────────────────────────

func TestNewServer_routesRegistered(t *testing.T) {
	srv := newTestServer(10)

	routes := []struct {
		path string
		code int
	}{
		{"/healthz", http.StatusOK},
		{"/tail", http.StatusUnauthorized}, // missing tenant
	}
	for _, r := range routes {
		req := httptest.NewRequest(http.MethodGet, r.path, nil)
		rr := httptest.NewRecorder()
		srv.ServeHTTP(rr, req)
		if rr.Code != r.code {
			t.Errorf("route %s: got %d, want %d", r.path, rr.Code, r.code)
		}
	}
}

// ── serialisation check ───────────────────────────────────────────────────────

func TestDispatch_entrySerializesCorrectly(t *testing.T) {
	srv := newTestServer(10)
	ch := make(chan models.LogEntry, 4)
	srv.register("t1", ch)

	now := time.Now().UTC().Truncate(time.Second)
	entry := models.LogEntry{
		LogID:          "abc123",
		TenantID:       "t1",
		Service:        "web",
		Level:          "WARN",
		Message:        "high latency",
		EventTimestamp: now,
	}
	srv.dispatch(entry)

	got := <-ch
	b, _ := json.Marshal(got)
	var out models.LogEntry
	_ = json.Unmarshal(b, &out)

	if out.LogID != "abc123" {
		t.Errorf("LogID round-trip: got %q", out.LogID)
	}
	if out.Level != "WARN" {
		t.Errorf("Level round-trip: got %q", out.Level)
	}
}

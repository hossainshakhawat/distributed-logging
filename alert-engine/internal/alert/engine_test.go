package alert

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hossainshakhawat/distributed-logging/store-kafka/kafka"
	"github.com/hossainshakhawat/distributed-logging/store-kafka/models"
)

// ── mock consumer ──────────────────────────────────────────────────────────────

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

// ── NewEngine ─────────────────────────────────────────────────────────────────

func TestNewEngine_notNil(t *testing.T) {
	e := NewEngine(newMockConsumer(), nil, "logs-normalized")
	if e == nil {
		t.Fatal("NewEngine returned nil")
	}
}

func TestNewEngine_stateInitialized(t *testing.T) {
	e := NewEngine(newMockConsumer(), []Rule{}, "logs-normalized")
	if e.state == nil {
		t.Fatal("state map should be initialized by NewEngine")
	}
}

// ── matches ───────────────────────────────────────────────────────────────────

func TestMatches_tenantExact(t *testing.T) {
	e := NewEngine(newMockConsumer(), nil, "logs-normalized")
	rule := Rule{TenantID: "acme"}
	tests := []struct {
		tenantID string
		want     bool
	}{
		{"acme", true},
		{"other", false},
		{"", false},
	}
	for _, tc := range tests {
		entry := models.LogEntry{TenantID: tc.tenantID}
		if got := e.matches(rule, entry); got != tc.want {
			t.Errorf("tenantID=%q: got %v, want %v", tc.tenantID, got, tc.want)
		}
	}
}

func TestMatches_tenantWildcard(t *testing.T) {
	e := NewEngine(newMockConsumer(), nil, "logs-normalized")
	rule := Rule{TenantID: "*"}
	for _, tid := range []string{"acme", "beta", ""} {
		entry := models.LogEntry{TenantID: tid}
		if !e.matches(rule, entry) {
			t.Errorf("wildcard tenant should match %q", tid)
		}
	}
}

func TestMatches_levelFilter(t *testing.T) {
	e := NewEngine(newMockConsumer(), nil, "logs-normalized")
	rule := Rule{TenantID: "t1", Level: "ERROR"}
	tests := []struct {
		level string
		want  bool
	}{
		{"ERROR", true},
		{"error", true}, // case-insensitive
		{"WARN", false},
		{"INFO", false},
		{"", false},
	}
	for _, tc := range tests {
		entry := models.LogEntry{TenantID: "t1", Level: tc.level}
		if got := e.matches(rule, entry); got != tc.want {
			t.Errorf("level=%q: got %v, want %v", tc.level, got, tc.want)
		}
	}
}

func TestMatches_emptyLevel_matchesAny(t *testing.T) {
	e := NewEngine(newMockConsumer(), nil, "logs-normalized")
	rule := Rule{TenantID: "t1", Level: ""}
	for _, lvl := range []string{"ERROR", "WARN", "INFO", "DEBUG", "FATAL", ""} {
		entry := models.LogEntry{TenantID: "t1", Level: lvl}
		if !e.matches(rule, entry) {
			t.Errorf("empty level rule should match any level %q", lvl)
		}
	}
}

func TestMatches_messageContains(t *testing.T) {
	e := NewEngine(newMockConsumer(), nil, "logs-normalized")
	rule := Rule{TenantID: "t1", MessageContains: "OOM"}
	tests := []struct {
		message string
		want    bool
	}{
		{"OOM killed", true},
		{"oom killed", true}, // case-insensitive
		{"out-of-memory", false},
		{"all good", false},
		{"", false},
	}
	for _, tc := range tests {
		entry := models.LogEntry{TenantID: "t1", Message: tc.message}
		if got := e.matches(rule, entry); got != tc.want {
			t.Errorf("message=%q: got %v, want %v", tc.message, got, tc.want)
		}
	}
}

func TestMatches_emptyMessageContains_matchesAny(t *testing.T) {
	e := NewEngine(newMockConsumer(), nil, "logs-normalized")
	rule := Rule{TenantID: "t1", MessageContains: ""}
	for _, msg := range []string{"hello", "error boom", "", "random"} {
		entry := models.LogEntry{TenantID: "t1", Message: msg}
		if !e.matches(rule, entry) {
			t.Errorf("empty MessageContains should match any message %q", msg)
		}
	}
}

func TestMatches_allFilters(t *testing.T) {
	e := NewEngine(newMockConsumer(), nil, "logs-normalized")
	rule := Rule{TenantID: "t1", Level: "ERROR", MessageContains: "crash"}
	// All conditions satisfied.
	if !e.matches(rule, models.LogEntry{TenantID: "t1", Level: "ERROR", Message: "app crash detected"}) {
		t.Fatal("expected match when all conditions are satisfied")
	}
	// TenantID wrong.
	if e.matches(rule, models.LogEntry{TenantID: "other", Level: "ERROR", Message: "crash"}) {
		t.Fatal("expected no match when tenantID differs")
	}
	// Level wrong.
	if e.matches(rule, models.LogEntry{TenantID: "t1", Level: "INFO", Message: "crash"}) {
		t.Fatal("expected no match when level differs")
	}
	// Message missing keyword.
	if e.matches(rule, models.LogEntry{TenantID: "t1", Level: "ERROR", Message: "all fine"}) {
		t.Fatal("expected no match when message missing keyword")
	}
}

// ── fire ──────────────────────────────────────────────────────────────────────

func TestFire_postsToWebhook(t *testing.T) {
	var calls atomic.Int32
	var receivedRule string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode payload: %v", err)
		}
		receivedRule, _ = payload["rule"].(string)
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	e := NewEngine(newMockConsumer(), nil, "logs-normalized")
	rule := Rule{Name: "test-rule", TenantID: "t1", Webhook: ts.URL}
	entry := models.LogEntry{TenantID: "t1", Level: "ERROR", Message: "boom"}
	e.fire(rule, entry, 3)

	if calls.Load() != 1 {
		t.Fatalf("expected 1 webhook call, got %d", calls.Load())
	}
	if receivedRule != "test-rule" {
		t.Errorf("expected rule name 'test-rule', got %q", receivedRule)
	}
}

func TestFire_webhookPayloadFields(t *testing.T) {
	var payload alertPayload
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&payload)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	e := NewEngine(newMockConsumer(), nil, "logs-normalized")
	rule := Rule{Name: "spike", TenantID: "acme", Webhook: ts.URL}
	entry := models.LogEntry{TenantID: "acme", Service: "svc-a", Level: "ERROR", Message: "timeout"}
	e.fire(rule, entry, 7)

	if payload.Rule != "spike" {
		t.Errorf("payload.Rule: got %q, want %q", payload.Rule, "spike")
	}
	if payload.TenantID != "acme" {
		t.Errorf("payload.TenantID: got %q, want %q", payload.TenantID, "acme")
	}
	if payload.Count != 7 {
		t.Errorf("payload.Count: got %d, want 7", payload.Count)
	}
	if payload.SampleMsg != "timeout" {
		t.Errorf("payload.SampleMsg: got %q, want %q", payload.SampleMsg, "timeout")
	}
}

func TestFire_badWebhookURL_doesNotPanic(t *testing.T) {
	e := NewEngine(newMockConsumer(), nil, "logs-normalized")
	rule := Rule{Name: "r", Webhook: "http://127.0.0.1:1/gone"}
	e.fire(rule, models.LogEntry{TenantID: "t1"}, 1)
	// No panic is sufficient.
}

// ── evaluate ──────────────────────────────────────────────────────────────────

func TestEvaluate_firesAtThreshold(t *testing.T) {
	var hits atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	rule := Rule{Name: "err-spike", TenantID: "t1", Level: "ERROR", Threshold: 3, WindowSecs: 60, Webhook: ts.URL}
	e := NewEngine(newMockConsumer(), []Rule{rule}, "logs-normalized")
	entry := models.LogEntry{TenantID: "t1", Level: "ERROR", Message: "boom"}

	e.evaluate(entry) // count=1
	e.evaluate(entry) // count=2
	if hits.Load() != 0 {
		t.Fatalf("should not fire before threshold; got %d fires", hits.Load())
	}

	e.evaluate(entry) // count=3 → fire
	time.Sleep(100 * time.Millisecond)
	if hits.Load() < 1 {
		t.Fatalf("expected at least 1 webhook fire at threshold, got %d", hits.Load())
	}
}

func TestEvaluate_doesNotFireBelowThreshold(t *testing.T) {
	var hits atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	rule := Rule{Name: "r", TenantID: "t1", Level: "ERROR", Threshold: 100, WindowSecs: 60, Webhook: ts.URL}
	e := NewEngine(newMockConsumer(), []Rule{rule}, "logs-normalized")
	entry := models.LogEntry{TenantID: "t1", Level: "ERROR", Message: "x"}

	for i := 0; i < 10; i++ {
		e.evaluate(entry)
	}
	time.Sleep(30 * time.Millisecond)
	if hits.Load() != 0 {
		t.Fatalf("expected no fires below threshold, got %d", hits.Load())
	}
}

func TestEvaluate_nonMatchingEntry_ignored(t *testing.T) {
	var hits atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	rule := Rule{Name: "r", TenantID: "t1", Level: "ERROR", Threshold: 1, WindowSecs: 60, Webhook: ts.URL}
	e := NewEngine(newMockConsumer(), []Rule{rule}, "logs-normalized")

	// Different tenant — should not match.
	e.evaluate(models.LogEntry{TenantID: "other", Level: "ERROR", Message: "x"})
	time.Sleep(30 * time.Millisecond)
	if hits.Load() != 0 {
		t.Fatalf("expected no fires for non-matching entry, got %d", hits.Load())
	}
}

func TestEvaluate_windowExpiry_resetsCount(t *testing.T) {
	// Use a high threshold so the webhook is never called during this test.
	rule := Rule{Name: "r", TenantID: "t1", Level: "ERROR", Threshold: 100, WindowSecs: 60}
	e := NewEngine(newMockConsumer(), []Rule{rule}, "logs-normalized")
	entry := models.LogEntry{TenantID: "t1", Level: "ERROR", Message: "x"}
	key := rule.Name + "|" + entry.TenantID

	// Call evaluate twice to accumulate count within the same window.
	e.evaluate(entry)
	e.evaluate(entry)
	e.mu.Lock()
	before := e.state[key].count
	e.mu.Unlock()
	if before != 2 {
		t.Fatalf("expected count=2 within window, got %d", before)
	}

	// Force the window to have expired.
	e.mu.Lock()
	e.state[key].windowEnd = time.Now().UTC().Add(-1 * time.Second)
	e.mu.Unlock()

	// Next evaluate should start a fresh window; count should reset to 1.
	e.evaluate(entry)
	e.mu.Lock()
	after := e.state[key].count
	e.mu.Unlock()
	if after != 1 {
		t.Errorf("after window expiry count should reset to 1, got %d", after)
	}
}

// ── Start / Stop ──────────────────────────────────────────────────────────────

func TestEngine_StartStop(t *testing.T) {
	consumer := newMockConsumer()
	e := NewEngine(consumer, []Rule{}, "logs-normalized")
	if err := e.Start(); err != nil {
		t.Fatalf("Start() error: %v", err)
	}
	// Stop should not block or panic.
	e.Stop()
}

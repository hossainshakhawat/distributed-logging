package agent

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hossainshakhawat/distributed-logging/store-kafka/models"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func newTestAgent(gatewayURL, bufDir string) *Agent {
	return New(Config{
		GatewayURL:    gatewayURL,
		TenantID:      "tenant-a",
		Service:       "test-svc",
		Host:          "host-1",
		Environment:   "test",
		BatchSize:     100,
		FlushInterval: 60,
		BufferDir:     bufDir,
	})
}

// ── extractLevel ─────────────────────────────────────────────────────────────

func TestExtractLevel(t *testing.T) {
	tests := []struct {
		line string
		want string
	}{
		{"[ERROR] something failed", "ERROR"},
		{"error: connection refused", "ERROR"},
		{"WARN: disk usage high", "WARN"},
		{"[warn] retrying", "WARN"},
		{"[INFO] started service", "INFO"},
		{"info: processing item", "INFO"},
		{"DEBUG trace value=42", "DEBUG"},
		// "error" appears in "unrecoverable error", so ERROR is matched first.
		{"[FATAL] unrecoverable error", "ERROR"},
		// Line without "error" substring: FATAL is matched.
		{"[FATAL] system crash", "FATAL"},
		{"fatal crash detected", "FATAL"},
		{"no level here", "INFO"}, // default
		{"", "INFO"},              // empty → default
		{"random text", "INFO"},   // no recognizable level → default
	}
	for _, tc := range tests {
		if got := extractLevel(tc.line); got != tc.want {
			t.Errorf("extractLevel(%q) = %q, want %q", tc.line, got, tc.want)
		}
	}
}

// ── containsIgnoreCase ────────────────────────────────────────────────────────

func TestContainsIgnoreCase(t *testing.T) {
	tests := []struct {
		s    string
		sub  string
		want bool
	}{
		{"ERROR: boom", "ERROR", true},
		{"error: boom", "ERROR", true},
		{"[Error]", "error", true},
		{"nothing here", "WARN", false},
		{"", "INFO", false},
		{"INFO", "INFO", true},
		{"info", "INFO", true},
	}
	for _, tc := range tests {
		if got := containsIgnoreCase(tc.s, tc.sub); got != tc.want {
			t.Errorf("containsIgnoreCase(%q, %q) = %v, want %v", tc.s, tc.sub, got, tc.want)
		}
	}
}

// ── ingest ────────────────────────────────────────────────────────────────────

func TestIngest_appendsEntry(t *testing.T) {
	a := newTestAgent("http://localhost", t.TempDir())
	a.ingest("INFO: startup complete")

	a.mu.Lock()
	n := len(a.pending)
	a.mu.Unlock()

	if n != 1 {
		t.Errorf("expected 1 pending entry, got %d", n)
	}
}

func TestIngest_setsCorrectFields(t *testing.T) {
	a := newTestAgent("http://localhost", t.TempDir())
	a.ingest("ERROR: database unavailable")

	a.mu.Lock()
	entry := a.pending[0]
	a.mu.Unlock()

	if entry.TenantID != "tenant-a" {
		t.Errorf("TenantID: got %q, want %q", entry.TenantID, "tenant-a")
	}
	if entry.Service != "test-svc" {
		t.Errorf("Service: got %q, want %q", entry.Service, "test-svc")
	}
	if entry.Level != "ERROR" {
		t.Errorf("Level: got %q, want %q", entry.Level, "ERROR")
	}
	if entry.Message != "ERROR: database unavailable" {
		t.Errorf("Message: got %q", entry.Message)
	}
	if entry.LogID == "" {
		t.Error("LogID should be set")
	}
}

func TestIngest_flushesOnBatchSize(t *testing.T) {
	// Set up a test server to capture ship calls.
	var shipCount atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		shipCount.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer ts.Close()

	a := New(Config{
		GatewayURL:    ts.URL,
		TenantID:      "t1",
		Service:       "svc",
		BatchSize:     3,
		FlushInterval: 60,
		BufferDir:     t.TempDir(),
	})

	a.ingest("line-1")
	a.ingest("line-2")
	// BatchSize=3, so no flush yet.
	if shipCount.Load() != 0 {
		t.Fatalf("expected no flush at 2 entries")
	}

	a.ingest("line-3") // should trigger flush
	// The flush goroutine runs asynchronously; wait a bit.
	time.Sleep(100 * time.Millisecond)
	if shipCount.Load() < 1 {
		t.Fatalf("expected flush after reaching BatchSize, got %d ships", shipCount.Load())
	}
}

// ── flush ─────────────────────────────────────────────────────────────────────

func TestFlush_emptyPending_noOp(t *testing.T) {
	a := newTestAgent("http://localhost", t.TempDir())
	// flush() with no pending should not panic or ship anything.
	a.flush()
	a.mu.Lock()
	n := len(a.pending)
	a.mu.Unlock()
	if n != 0 {
		t.Errorf("expected 0 pending after flush, got %d", n)
	}
}

func TestFlush_drainsPending(t *testing.T) {
	var received atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer ts.Close()

	a := newTestAgent(ts.URL, t.TempDir())
	a.pending = []models.LogEntry{
		{LogID: "l1", TenantID: "tenant-a", Service: "test-svc", Message: "hello"},
	}
	a.flush()

	if received.Load() != 1 {
		t.Errorf("expected 1 ship call, got %d", received.Load())
	}
	a.mu.Lock()
	n := len(a.pending)
	a.mu.Unlock()
	if n != 0 {
		t.Errorf("expected pending to be drained, got %d", n)
	}
}

func TestFlush_buffersToDiskOnShipFailure(t *testing.T) {
	// Point to a non-existent server so ship fails.
	bufDir := t.TempDir()
	a := newTestAgent("http://127.0.0.1:1/no-server", bufDir)
	a.pending = []models.LogEntry{
		{LogID: "l1", TenantID: "tenant-a", Service: "test-svc", Message: "fail-me"},
	}
	a.flush()

	entries, err := os.ReadDir(bufDir)
	if err != nil {
		t.Fatalf("read buffer dir: %v", err)
	}
	if len(entries) == 0 {
		t.Error("expected at least one buffered file on ship failure")
	}
}

// ── ship ──────────────────────────────────────────────────────────────────────

func TestShip_success(t *testing.T) {
	var gotTenantID string
	var gotBatch models.LogBatch
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTenantID = r.Header.Get("X-Tenant-ID")
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Errorf("gzip reader: %v", err)
			return
		}
		b, _ := io.ReadAll(gz)
		_ = json.Unmarshal(b, &gotBatch)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer ts.Close()

	a := newTestAgent(ts.URL, t.TempDir())
	batch := models.LogBatch{
		AgentID: a.agentID,
		Entries: []models.LogEntry{
			{LogID: "l1", TenantID: "tenant-a", Service: "test-svc", Message: "hi"},
		},
		SentAt: time.Now().UTC(),
	}
	if err := a.ship(batch); err != nil {
		t.Fatalf("ship error: %v", err)
	}
	if gotTenantID != "tenant-a" {
		t.Errorf("X-Tenant-ID: got %q, want %q", gotTenantID, "tenant-a")
	}
	if len(gotBatch.Entries) != 1 {
		t.Errorf("expected 1 entry in shipped batch, got %d", len(gotBatch.Entries))
	}
}

func TestShip_serverError_returnsError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}))
	defer ts.Close()

	a := newTestAgent(ts.URL, t.TempDir())
	batch := models.LogBatch{
		Entries: []models.LogEntry{{LogID: "l1", TenantID: "t1", Message: "x"}},
	}
	err := a.ship(batch)
	if err == nil {
		t.Fatal("expected error for 5xx response")
	}
}

func TestShip_connectionRefused_returnsError(t *testing.T) {
	a := newTestAgent("http://127.0.0.1:1/unreachable", t.TempDir())
	batch := models.LogBatch{
		Entries: []models.LogEntry{{LogID: "l1", TenantID: "t1", Message: "x"}},
	}
	if err := a.ship(batch); err == nil {
		t.Fatal("expected error when gateway is unreachable")
	}
}

func TestShip_usesGzipEncoding(t *testing.T) {
	var encoding string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		encoding = r.Header.Get("Content-Encoding")
		w.WriteHeader(http.StatusAccepted)
	}))
	defer ts.Close()

	a := newTestAgent(ts.URL, t.TempDir())
	_ = a.ship(models.LogBatch{Entries: []models.LogEntry{{TenantID: "t1", Message: "x"}}})
	if encoding != "gzip" {
		t.Errorf("expected Content-Encoding gzip, got %q", encoding)
	}
}

// ── bufferToDisk / replayBuffer ───────────────────────────────────────────────

func TestBufferToDisk_writesFile(t *testing.T) {
	bufDir := t.TempDir()
	a := newTestAgent("http://localhost", bufDir)
	batch := models.LogBatch{
		AgentID: "a1",
		Entries: []models.LogEntry{{LogID: "l1", TenantID: "t1", Message: "buffered"}},
	}
	a.bufferToDisk(batch)

	entries, err := os.ReadDir(bufDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 buffered file, got %d", len(entries))
	}

	// Verify file content is valid JSON.
	raw, _ := os.ReadFile(filepath.Join(bufDir, entries[0].Name()))
	var got models.LogBatch
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("buffered file is not valid JSON: %v", err)
	}
	if got.AgentID != "a1" {
		t.Errorf("AgentID: got %q, want %q", got.AgentID, "a1")
	}
}

func TestReplayBuffer_retriesAndRemovesOnSuccess(t *testing.T) {
	var received atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		received.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer ts.Close()

	bufDir := t.TempDir()
	a := newTestAgent(ts.URL, bufDir)

	// Write a batch file manually.
	batch := models.LogBatch{
		AgentID: "replay-agent",
		Entries: []models.LogEntry{{LogID: "l1", TenantID: "tenant-a", Message: "replay me"}},
	}
	a.bufferToDisk(batch)

	// Confirm file exists.
	before, _ := os.ReadDir(bufDir)
	if len(before) != 1 {
		t.Fatalf("expected 1 buffer file before replay, got %d", len(before))
	}

	a.replayBuffer()

	if received.Load() < 1 {
		t.Error("expected at least one ship call during replay")
	}
	// File should be removed on success.
	after, _ := os.ReadDir(bufDir)
	if len(after) != 0 {
		t.Errorf("expected buffer file removed after successful replay, got %d files", len(after))
	}
}

func TestReplayBuffer_keepsFileOnFailure(t *testing.T) {
	bufDir := t.TempDir()
	// Gateway URL that refuses connections.
	a := newTestAgent("http://127.0.0.1:1/unreachable", bufDir)

	batch := models.LogBatch{
		Entries: []models.LogEntry{{LogID: "l1", TenantID: "t1", Message: "no-ship"}},
	}
	a.bufferToDisk(batch)

	a.replayBuffer()

	entries, _ := os.ReadDir(bufDir)
	if len(entries) == 0 {
		t.Error("buffer file should remain when ship fails during replay")
	}
}

// ── Start / Stop ──────────────────────────────────────────────────────────────

func TestStart_createsBufferDir(t *testing.T) {
	bufDir := filepath.Join(t.TempDir(), "subdir", "buffer")
	a := New(Config{
		GatewayURL:    "http://localhost",
		TenantID:      "t1",
		Service:       "svc",
		BatchSize:     10,
		FlushInterval: 60,
		BufferDir:     bufDir,
	})
	if err := a.Start(); err != nil {
		t.Fatalf("Start() error: %v", err)
	}
	a.Stop()
	if _, err := os.Stat(bufDir); os.IsNotExist(err) {
		t.Error("Start() should create BufferDir")
	}
}

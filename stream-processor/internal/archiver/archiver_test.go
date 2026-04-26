package archiver

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hossainshakhawat/distributed-logging/store-kafka/models"
)

// ── mock Uploader ─────────────────────────────────────────────────────────────

type mockUploader struct {
	mu      sync.Mutex
	uploads []uploadCall
	callErr error
}

type uploadCall struct {
	key     string
	entries []models.LogEntry
}

func (m *mockUploader) Upload(_ context.Context, key string, entries []models.LogEntry) error {
	m.mu.Lock()
	cp := make([]models.LogEntry, len(entries))
	copy(cp, entries)
	m.uploads = append(m.uploads, uploadCall{key: key, entries: cp})
	m.mu.Unlock()
	return m.callErr
}

func (m *mockUploader) totalUploaded() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, u := range m.uploads {
		n += len(u.entries)
	}
	return n
}

func (m *mockUploader) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.uploads)
}

// ── Add ───────────────────────────────────────────────────────────────────────

func TestAdd_buffersEntry(t *testing.T) {
	client := &mockUploader{}
	// Large batchSize so flush never triggers automatically.
	arch := New(client, 1000, time.Hour)
	defer arch.Stop()

	arch.Add(models.LogEntry{LogID: "l1", TenantID: "t1", Message: "hello"})

	arch.mu.Lock()
	n := len(arch.buf)
	arch.mu.Unlock()

	if n != 1 {
		t.Errorf("expected 1 buffered entry, got %d", n)
	}
}

func TestAdd_multipleEntries(t *testing.T) {
	client := &mockUploader{}
	arch := New(client, 1000, time.Hour)
	defer arch.Stop()

	for i := 0; i < 4; i++ {
		arch.Add(models.LogEntry{LogID: "l", TenantID: "t1", Message: "msg"})
	}

	arch.mu.Lock()
	n := len(arch.buf)
	arch.mu.Unlock()

	if n != 4 {
		t.Errorf("expected 4 buffered entries, got %d", n)
	}
}

func TestAdd_triggersFlushAtBatchSize(t *testing.T) {
	client := &mockUploader{}
	arch := New(client, 3, time.Hour)
	defer arch.Stop()

	arch.Add(models.LogEntry{LogID: "l1", TenantID: "t1", Message: "a"})
	arch.Add(models.LogEntry{LogID: "l2", TenantID: "t1", Message: "b"})
	arch.Add(models.LogEntry{LogID: "l3", TenantID: "t1", Message: "c"}) // triggers flush

	time.Sleep(100 * time.Millisecond)

	if client.totalUploaded() < 3 {
		t.Errorf("expected 3 entries uploaded after batch flush, got %d", client.totalUploaded())
	}
}

func TestAdd_drainsBufAfterFlush(t *testing.T) {
	client := &mockUploader{}
	arch := New(client, 2, time.Hour)
	defer arch.Stop()

	arch.Add(models.LogEntry{LogID: "l1", TenantID: "t1", Message: "a"})
	arch.Add(models.LogEntry{LogID: "l2", TenantID: "t1", Message: "b"}) // triggers flush

	time.Sleep(100 * time.Millisecond)

	arch.mu.Lock()
	remaining := len(arch.buf)
	arch.mu.Unlock()

	if remaining != 0 {
		t.Errorf("buffer should be empty after flush, got %d entries", remaining)
	}
}

// ── flush ─────────────────────────────────────────────────────────────────────

func TestFlush_emptyBuffer_noUpload(t *testing.T) {
	client := &mockUploader{}
	arch := New(client, 100, time.Hour)
	defer arch.Stop()

	arch.flush()

	if client.callCount() != 0 {
		t.Errorf("flush on empty buffer should not call Upload, got %d calls", client.callCount())
	}
}

func TestFlush_callsUpload(t *testing.T) {
	client := &mockUploader{}
	arch := New(client, 100, time.Hour)
	defer arch.Stop()

	arch.buf = []models.LogEntry{
		{LogID: "l1", TenantID: "t1", Message: "a"},
		{LogID: "l2", TenantID: "t1", Message: "b"},
	}
	arch.flush()

	if client.totalUploaded() != 2 {
		t.Errorf("expected 2 uploaded entries, got %d", client.totalUploaded())
	}
}

func TestFlush_clearsBuf(t *testing.T) {
	client := &mockUploader{}
	arch := New(client, 100, time.Hour)
	defer arch.Stop()

	arch.buf = []models.LogEntry{{LogID: "l1", TenantID: "t1", Message: "msg"}}
	arch.flush()

	arch.mu.Lock()
	n := len(arch.buf)
	arch.mu.Unlock()

	if n != 0 {
		t.Errorf("buf should be cleared after flush, got %d", n)
	}
}

func TestFlush_groupsByTenant(t *testing.T) {
	client := &mockUploader{}
	arch := New(client, 100, time.Hour)
	defer arch.Stop()

	arch.buf = []models.LogEntry{
		{LogID: "l1", TenantID: "acme", Message: "a"},
		{LogID: "l2", TenantID: "beta", Message: "b"},
		{LogID: "l3", TenantID: "acme", Message: "c"},
	}
	arch.flush()

	// Should produce 2 upload calls (one per tenant).
	if client.callCount() != 2 {
		t.Errorf("expected 2 upload calls (one per tenant), got %d", client.callCount())
	}

	// Verify tenant grouping.
	tenantCounts := make(map[string]int)
	client.mu.Lock()
	for _, u := range client.uploads {
		for _, e := range u.entries {
			tenantCounts[e.TenantID] += 1
		}
	}
	client.mu.Unlock()

	if tenantCounts["acme"] != 2 {
		t.Errorf("acme: expected 2 entries, got %d", tenantCounts["acme"])
	}
	if tenantCounts["beta"] != 1 {
		t.Errorf("beta: expected 1 entry, got %d", tenantCounts["beta"])
	}
}

func TestFlush_s3KeyContainsTenantID(t *testing.T) {
	client := &mockUploader{}
	arch := New(client, 100, time.Hour)
	defer arch.Stop()

	arch.buf = []models.LogEntry{
		{LogID: "l1", TenantID: "myorg", Message: "x"},
	}
	arch.flush()

	client.mu.Lock()
	calls := client.uploads
	client.mu.Unlock()

	if len(calls) == 0 {
		t.Fatal("expected at least one upload call")
	}
	if len(calls[0].key) == 0 {
		t.Error("S3 key should not be empty")
	}
	// The key is produced by s3client.ArchivePath which starts with the tenantID.
	if calls[0].key[:len("myorg")] != "myorg" {
		t.Errorf("S3 key should start with tenantID, got %q", calls[0].key)
	}
}

// ── Stop ──────────────────────────────────────────────────────────────────────

func TestStop_flushesRemainingEntries(t *testing.T) {
	client := &mockUploader{}
	arch := New(client, 1000, time.Hour) // large batchSize, no auto-flush

	arch.Add(models.LogEntry{LogID: "l1", TenantID: "t1", Message: "pending"})
	arch.Add(models.LogEntry{LogID: "l2", TenantID: "t1", Message: "also pending"})

	arch.Stop() // should flush remaining entries

	if client.totalUploaded() != 2 {
		t.Errorf("Stop should flush pending entries: got %d uploaded", client.totalUploaded())
	}
}

// ── ticker-driven flush ───────────────────────────────────────────────────────

func TestFlushLoop_tickerFlushes(t *testing.T) {
	client := &mockUploader{}
	arch := New(client, 1000, 50*time.Millisecond) // fast ticker
	defer arch.Stop()

	arch.Add(models.LogEntry{LogID: "l1", TenantID: "t1", Message: "tick-flush"})

	time.Sleep(150 * time.Millisecond)

	if client.totalUploaded() < 1 {
		t.Errorf("ticker should have flushed entry, got %d uploaded", client.totalUploaded())
	}
}

// ── concurrent safety ─────────────────────────────────────────────────────────

func TestAdd_concurrentSafety(t *testing.T) {
	client := &mockUploader{}
	arch := New(client, 1000, time.Hour)
	defer arch.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			arch.Add(models.LogEntry{LogID: "l", TenantID: "t1", Message: "concurrent"})
		}()
	}
	wg.Wait()

	arch.mu.Lock()
	n := len(arch.buf)
	arch.mu.Unlock()

	if n != 50 {
		t.Errorf("concurrent Add: expected 50 buffered, got %d", n)
	}
}

// ── partSeq increments ────────────────────────────────────────────────────────

func TestFlush_partSeqIncrements(t *testing.T) {
	client := &mockUploader{}
	arch := New(client, 100, time.Hour)
	defer arch.Stop()

	// Two separate flushes with the same tenant should use different part numbers.
	arch.buf = []models.LogEntry{{LogID: "l1", TenantID: "t1", Message: "a"}}
	arch.flush()
	arch.buf = []models.LogEntry{{LogID: "l2", TenantID: "t1", Message: "b"}}
	arch.flush()

	client.mu.Lock()
	calls := client.uploads
	client.mu.Unlock()

	if len(calls) != 2 {
		t.Fatalf("expected 2 upload calls, got %d", len(calls))
	}
	if calls[0].key == calls[1].key {
		t.Errorf("consecutive flushes should produce distinct S3 keys, both got %q", calls[0].key)
	}
}

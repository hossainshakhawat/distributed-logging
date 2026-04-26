package indexer

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hossainshakhawat/distributed-logging/store-kafka/models"
)

// ── mock BulkIndexer ──────────────────────────────────────────────────────────

type mockIndexer struct {
	mu      sync.Mutex
	calls   [][]models.LogEntry
	callErr error
}

func (m *mockIndexer) IndexBulk(_ context.Context, entries []models.LogEntry) error {
	m.mu.Lock()
	cp := make([]models.LogEntry, len(entries))
	copy(cp, entries)
	m.calls = append(m.calls, cp)
	m.mu.Unlock()
	return m.callErr
}

func (m *mockIndexer) totalIndexed() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, batch := range m.calls {
		n += len(batch)
	}
	return n
}

// ── Add ───────────────────────────────────────────────────────────────────────

func TestAdd_buffersEntry(t *testing.T) {
	client := &mockIndexer{}
	// Large batchSize so flush never triggers automatically.
	idx := New(client, 1000, time.Hour)
	defer idx.Stop()

	idx.Add(models.LogEntry{LogID: "l1", TenantID: "t1", Message: "hello"})

	idx.mu.Lock()
	n := len(idx.buf)
	idx.mu.Unlock()

	if n != 1 {
		t.Errorf("expected 1 buffered entry, got %d", n)
	}
}

func TestAdd_multipleEntries(t *testing.T) {
	client := &mockIndexer{}
	idx := New(client, 1000, time.Hour)
	defer idx.Stop()

	for i := 0; i < 5; i++ {
		idx.Add(models.LogEntry{LogID: "l", TenantID: "t1", Message: "msg"})
	}

	idx.mu.Lock()
	n := len(idx.buf)
	idx.mu.Unlock()

	if n != 5 {
		t.Errorf("expected 5 buffered entries, got %d", n)
	}
}

func TestAdd_triggersFlushAtBatchSize(t *testing.T) {
	client := &mockIndexer{}
	idx := New(client, 3, time.Hour)
	defer idx.Stop()

	idx.Add(models.LogEntry{LogID: "l1", TenantID: "t1", Message: "a"})
	idx.Add(models.LogEntry{LogID: "l2", TenantID: "t1", Message: "b"})
	// At batchSize=3, third Add triggers async flush.
	idx.Add(models.LogEntry{LogID: "l3", TenantID: "t1", Message: "c"})

	// Wait for the async flush goroutine.
	time.Sleep(100 * time.Millisecond)

	if client.totalIndexed() < 3 {
		t.Errorf("expected 3 entries indexed after batch flush, got %d", client.totalIndexed())
	}
}

func TestAdd_drainsBufAfterFlush(t *testing.T) {
	client := &mockIndexer{}
	idx := New(client, 2, time.Hour)
	defer idx.Stop()

	idx.Add(models.LogEntry{LogID: "l1", TenantID: "t1", Message: "a"})
	idx.Add(models.LogEntry{LogID: "l2", TenantID: "t1", Message: "b"}) // triggers flush

	time.Sleep(100 * time.Millisecond)

	idx.mu.Lock()
	remaining := len(idx.buf)
	idx.mu.Unlock()

	if remaining != 0 {
		t.Errorf("buffer should be empty after flush, got %d entries", remaining)
	}
}

// ── flush ─────────────────────────────────────────────────────────────────────

func TestFlush_emptyBuffer_noCall(t *testing.T) {
	client := &mockIndexer{}
	idx := New(client, 100, time.Hour)
	defer idx.Stop()

	idx.flush()

	client.mu.Lock()
	n := len(client.calls)
	client.mu.Unlock()

	if n != 0 {
		t.Errorf("flush on empty buffer should not call IndexBulk, got %d calls", n)
	}
}

func TestFlush_callsIndexBulk(t *testing.T) {
	client := &mockIndexer{}
	idx := New(client, 100, time.Hour)
	defer idx.Stop()

	idx.buf = []models.LogEntry{
		{LogID: "l1", TenantID: "t1", Message: "a"},
		{LogID: "l2", TenantID: "t1", Message: "b"},
	}
	idx.flush()

	if client.totalIndexed() != 2 {
		t.Errorf("expected 2 indexed entries, got %d", client.totalIndexed())
	}
}

func TestFlush_clearsBuf(t *testing.T) {
	client := &mockIndexer{}
	idx := New(client, 100, time.Hour)
	defer idx.Stop()

	idx.buf = []models.LogEntry{{LogID: "l1", TenantID: "t1", Message: "msg"}}
	idx.flush()

	idx.mu.Lock()
	n := len(idx.buf)
	idx.mu.Unlock()

	if n != 0 {
		t.Errorf("buf should be cleared after flush, got %d", n)
	}
}

// ── Stop ──────────────────────────────────────────────────────────────────────

func TestStop_flushesPendingEntries(t *testing.T) {
	client := &mockIndexer{}
	idx := New(client, 1000, time.Hour) // large batchSize, no auto-flush

	idx.Add(models.LogEntry{LogID: "l1", TenantID: "t1", Message: "pending"})
	idx.Add(models.LogEntry{LogID: "l2", TenantID: "t1", Message: "also pending"})

	idx.Stop() // should flush remaining entries

	if client.totalIndexed() != 2 {
		t.Errorf("Stop should flush pending entries: got %d indexed", client.totalIndexed())
	}
}

// ── ticker-driven flush ───────────────────────────────────────────────────────

func TestFlushLoop_tickerFlushes(t *testing.T) {
	var flushCount atomic.Int32
	client := &mockIndexer{}
	// Very short flush interval.
	idx := New(client, 1000, 50*time.Millisecond)
	defer idx.Stop()

	idx.Add(models.LogEntry{LogID: "l1", TenantID: "t1", Message: "tick-flush"})

	time.Sleep(150 * time.Millisecond)

	_ = flushCount.Load()
	if client.totalIndexed() < 1 {
		t.Errorf("ticker should have flushed entry, got %d indexed", client.totalIndexed())
	}
}

// ── concurrent safety ─────────────────────────────────────────────────────────

func TestAdd_concurrentSafety(t *testing.T) {
	client := &mockIndexer{}
	idx := New(client, 1000, time.Hour)
	defer idx.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			idx.Add(models.LogEntry{LogID: "l", TenantID: "t1", Message: "concurrent"})
		}(i)
	}
	wg.Wait()

	idx.mu.Lock()
	n := len(idx.buf)
	idx.mu.Unlock()

	if n != 50 {
		t.Errorf("concurrent Add: expected 50 buffered, got %d", n)
	}
}

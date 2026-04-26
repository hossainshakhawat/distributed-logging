package archiver

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hossainshakhawat/distributed-logging/store-kafka/models"
	s3client "github.com/hossainshakhawat/distributed-logging/store-s3/client"
)

// Uploader is the interface for uploading log entries to object storage, satisfied by *s3client.Client.
type Uploader interface {
	Upload(ctx context.Context, key string, entries []models.LogEntry) error
}

// Archiver batches log entries and uploads them to S3 as gzip-compressed NDJSON.
type Archiver struct {
	client    Uploader
	buf       []models.LogEntry
	mu        sync.Mutex
	partSeq   atomic.Int64
	batchSize int
	ticker    *time.Ticker
	done      chan struct{}
}

// New creates an Archiver that flushes to S3 when batchSize entries accumulate
// or every flushInterval, whichever comes first.
func New(client Uploader, batchSize int, flushInterval time.Duration) *Archiver {
	a := &Archiver{
		client:    client,
		batchSize: batchSize,
		done:      make(chan struct{}),
	}
	a.ticker = time.NewTicker(flushInterval)
	go a.flushLoop()
	return a
}

// Add queues a single entry for archiving.
func (a *Archiver) Add(entry models.LogEntry) {
	a.mu.Lock()
	a.buf = append(a.buf, entry)
	shouldFlush := len(a.buf) >= a.batchSize
	a.mu.Unlock()
	if shouldFlush {
		go a.flush()
	}
}

// Stop flushes remaining entries and stops the background ticker.
func (a *Archiver) Stop() {
	close(a.done)
	a.flush()
}

func (a *Archiver) flushLoop() {
	for {
		select {
		case <-a.ticker.C:
			a.flush()
		case <-a.done:
			a.ticker.Stop()
			return
		}
	}
}

func (a *Archiver) flush() {
	a.mu.Lock()
	if len(a.buf) == 0 {
		a.mu.Unlock()
		return
	}
	batch := a.buf
	a.buf = nil
	a.mu.Unlock()

	// Group by tenant so each tenant's data lands in its own S3 path.
	byTenant := make(map[string][]models.LogEntry)
	for _, e := range batch {
		byTenant[e.TenantID] = append(byTenant[e.TenantID], e)
	}

	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for tenantID, entries := range byTenant {
		part := int(a.partSeq.Add(1))
		key := s3client.ArchivePath(tenantID, now, part)
		if err := a.client.Upload(ctx, key, entries); err != nil {
			log.Printf("archiver: upload tenant=%s key=%s err=%v", tenantID, key, err)
			continue
		}
		log.Printf("archiver: archived %d entries tenant=%s key=%s",
			len(entries), tenantID, fmt.Sprintf("s3://%s", key))
	}
}

package indexer

import (
	"context"
	"log"
	"sync"
	"time"

	osclient "github.com/hossainshakhawat/distributed-logging/store-opensearch/client"
	"github.com/hossainshakhawat/distributed-logging/store-kafka/models"
)

// Indexer batches log entries and bulk-indexes them to OpenSearch.
type Indexer struct {
	client      *osclient.Client
	buf         []models.LogEntry
	mu          sync.Mutex
	flushSize   int
	flushTicker *time.Ticker
	done        chan struct{}
}

// New creates an Indexer that flushes when batchSize entries accumulate
// or every flushInterval, whichever comes first.
func New(client *osclient.Client, batchSize int, flushInterval time.Duration) *Indexer {
	idx := &Indexer{
		client:    client,
		flushSize: batchSize,
		done:      make(chan struct{}),
	}
	idx.flushTicker = time.NewTicker(flushInterval)
	go idx.flushLoop()
	return idx
}

// Add queues a single entry for indexing.
func (i *Indexer) Add(entry models.LogEntry) {
	i.mu.Lock()
	i.buf = append(i.buf, entry)
	shouldFlush := len(i.buf) >= i.flushSize
	i.mu.Unlock()
	if shouldFlush {
		go i.flush()
	}
}

// Stop flushes remaining entries and stops the background ticker.
func (i *Indexer) Stop() {
	close(i.done)
	i.flush()
}

func (i *Indexer) flushLoop() {
	for {
		select {
		case <-i.flushTicker.C:
			i.flush()
		case <-i.done:
			i.flushTicker.Stop()
			return
		}
	}
}

func (i *Indexer) flush() {
	i.mu.Lock()
	if len(i.buf) == 0 {
		i.mu.Unlock()
		return
	}
	batch := i.buf
	i.buf = nil
	i.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := i.client.IndexBulk(ctx, batch); err != nil {
		log.Printf("indexer: bulk index error: %v", err)
	}
}

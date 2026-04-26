package agent

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/distributed-logging/shared/models"
	"github.com/google/uuid"
)

// Config holds all agent configuration.
type Config struct {
	GatewayURL    string
	TenantID      string
	Service       string
	Host          string
	Environment   string
	WatchPaths    []string
	BatchSize     int
	FlushInterval int // seconds
	BufferDir     string
}

// Agent watches log files, batches entries, and ships to the gateway.
type Agent struct {
	cfg     Config
	pending []models.LogEntry
	mu      sync.Mutex
	done    chan struct{}
	client  *http.Client
	agentID string
}

// New creates a new Agent.
func New(cfg Config) *Agent {
	return &Agent{
		cfg:     cfg,
		done:    make(chan struct{}),
		client:  &http.Client{Timeout: 10 * time.Second},
		agentID: uuid.NewString(),
	}
}

// Start begins tailing all configured paths and the flush loop.
func (a *Agent) Start() error {
	if err := os.MkdirAll(a.cfg.BufferDir, 0o700); err != nil {
		return fmt.Errorf("buffer dir: %w", err)
	}
	for _, p := range a.cfg.WatchPaths {
		go a.tail(p)
	}
	go a.flushLoop()
	// Replay any buffered batches from previous crashes.
	go a.replayBuffer()
	return nil
}

// Stop signals the agent to stop.
func (a *Agent) Stop() {
	close(a.done)
}

// tail reads new lines appended to path and ingests them.
func (a *Agent) tail(path string) {
	f, err := os.Open(path)
	if err != nil {
		log.Printf("agent: cannot open %s: %v", path, err)
		return
	}
	defer f.Close()
	// Seek to end so we only process new lines.
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		log.Printf("agent: seek %s: %v", path, err)
		return
	}
	scanner := bufio.NewScanner(f)
	for {
		select {
		case <-a.done:
			return
		default:
		}
		for scanner.Scan() {
			line := scanner.Text()
			a.ingest(line)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// ingest converts a raw log line into a LogEntry and buffers it.
func (a *Agent) ingest(line string) {
	entry := models.LogEntry{
		LogID:           a.logID(line),
		TenantID:        a.cfg.TenantID,
		Service:         a.cfg.Service,
		Host:            a.cfg.Host,
		Environment:     a.cfg.Environment,
		Level:           extractLevel(line),
		Message:         line,
		EventTimestamp:  time.Now().UTC(),
		IngestTimestamp: time.Now().UTC(),
	}
	a.mu.Lock()
	a.pending = append(a.pending, entry)
	shouldFlush := len(a.pending) >= a.cfg.BatchSize
	a.mu.Unlock()
	if shouldFlush {
		go a.flush()
	}
}

// logID deterministically hashes an agent+line pair.
func (a *Agent) logID(line string) string {
	h := sha256.Sum256([]byte(a.agentID + line + time.Now().String()))
	return fmt.Sprintf("%x", h[:8])
}

// flushLoop periodically ships pending entries.
func (a *Agent) flushLoop() {
	ticker := time.NewTicker(time.Duration(a.cfg.FlushInterval) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			a.flush()
		case <-a.done:
			a.flush()
			return
		}
	}
}

// flush drains pending entries and ships them to the gateway.
func (a *Agent) flush() {
	a.mu.Lock()
	if len(a.pending) == 0 {
		a.mu.Unlock()
		return
	}
	batch := models.LogBatch{
		AgentID: a.agentID,
		Entries: a.pending,
		SentAt:  time.Now().UTC(),
	}
	a.pending = nil
	a.mu.Unlock()

	if err := a.ship(batch); err != nil {
		log.Printf("agent: ship failed (%v), buffering locally", err)
		a.bufferToDisk(batch)
	}
}

// ship compresses and POSTs the batch to the gateway.
func (a *Agent) ship(batch models.LogBatch) error {
	payload, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err = gz.Write(payload); err != nil {
		return err
	}
	gz.Close()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		a.cfg.GatewayURL+"/ingest", &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", a.cfg.TenantID)

	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("gateway returned %d", resp.StatusCode)
	}
	return nil
}

// bufferToDisk saves the batch as a JSON file for later replay.
func (a *Agent) bufferToDisk(batch models.LogBatch) {
	name := filepath.Join(a.cfg.BufferDir, fmt.Sprintf("%d.json", time.Now().UnixNano()))
	b, _ := json.Marshal(batch)
	if err := os.WriteFile(name, b, 0o600); err != nil {
		log.Printf("agent: buffer write: %v", err)
	}
}

// replayBuffer retries any disk-buffered batches on startup.
func (a *Agent) replayBuffer() {
	entries, err := os.ReadDir(a.cfg.BufferDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		path := filepath.Join(a.cfg.BufferDir, e.Name())
		b, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var batch models.LogBatch
		if json.Unmarshal(b, &batch) != nil {
			continue
		}
		if err := a.ship(batch); err == nil {
			os.Remove(path)
		}
	}
}

// extractLevel does a best-effort extraction of log level from a line.
func extractLevel(line string) string {
	for _, lvl := range []string{"ERROR", "WARN", "INFO", "DEBUG", "FATAL"} {
		if len(line) > 0 {
			if containsIgnoreCase(line, lvl) {
				return lvl
			}
		}
	}
	return "INFO"
}

func containsIgnoreCase(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr ||
			len(s) > 0 && containsBytes([]byte(s), []byte(substr)))
}

func containsBytes(s, sub []byte) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		match := true
		for j := range sub {
			if s[i+j]|0x20 != sub[j]|0x20 {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

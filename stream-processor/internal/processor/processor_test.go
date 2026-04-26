package processor

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/hossainshakhawat/distributed-logging/store-kafka/kafka"
	"github.com/hossainshakhawat/distributed-logging/store-kafka/models"
)

// ── mock Kafka consumer ───────────────────────────────────────────────────────

type mockConsumer struct {
	msgs      chan *kafka.Message
	committed []*kafka.Message
	mu        sync.Mutex
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
func (m *mockConsumer) Commit(_ context.Context, msg *kafka.Message) error {
	m.mu.Lock()
	m.committed = append(m.committed, msg)
	m.mu.Unlock()
	return nil
}
func (m *mockConsumer) Close() error { return nil }

// ── mock Kafka producer ───────────────────────────────────────────────────────

type mockProducer struct {
	mu        sync.Mutex
	published []kafka.Message
}

func (m *mockProducer) Publish(_ context.Context, msg kafka.Message) error {
	m.mu.Lock()
	m.published = append(m.published, msg)
	m.mu.Unlock()
	return nil
}
func (m *mockProducer) Close() error { return nil }

func (m *mockProducer) messages() []kafka.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]kafka.Message, len(m.published))
	copy(cp, m.published)
	return cp
}

// ── mock dedup store ──────────────────────────────────────────────────────────

type mockDedup struct {
	mu   sync.Mutex
	keys map[string]struct{}
}

func newMockDedup() *mockDedup { return &mockDedup{keys: make(map[string]struct{})} }

func (m *mockDedup) SetNX(_ context.Context, key string, _ time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.keys[key]; exists {
		return false, nil // duplicate
	}
	m.keys[key] = struct{}{}
	return true, nil // new
}

// ── helpers ───────────────────────────────────────────────────────────────────

func newProcessor(prod *mockProducer, dedup DedupStore) *Processor {
	cfg := Config{
		ConsumerGroup: "test-group",
		DLQTopic:      kafka.TopicDeadLetter,
		DedupTTL:      time.Minute,
	}
	return New(cfg, newMockConsumer(), prod, nil, nil, dedup)
}

func batchMsg(batch models.LogBatch) *kafka.Message {
	b, _ := json.Marshal(batch)
	return &kafka.Message{Value: b}
}

// ── normalise ─────────────────────────────────────────────────────────────────

func TestNormalise_valid(t *testing.T) {
	p := newProcessor(&mockProducer{}, nil)
	entry := models.LogEntry{
		TenantID: "acme",
		Message:  "startup complete",
		Level:    "  info  ",
	}
	got, ok := p.normalise(entry)
	if !ok {
		t.Fatal("expected normalise to succeed")
	}
	if got.Level != "INFO" {
		t.Errorf("Level should be uppercased/trimmed: got %q", got.Level)
	}
}

func TestNormalise_missingTenantID_fails(t *testing.T) {
	p := newProcessor(&mockProducer{}, nil)
	entry := models.LogEntry{Message: "hello", Level: "INFO"}
	_, ok := p.normalise(entry)
	if ok {
		t.Fatal("expected normalise to fail for empty TenantID")
	}
}

func TestNormalise_missingMessage_fails(t *testing.T) {
	p := newProcessor(&mockProducer{}, nil)
	entry := models.LogEntry{TenantID: "t1", Level: "INFO"}
	_, ok := p.normalise(entry)
	if ok {
		t.Fatal("expected normalise to fail for empty Message")
	}
}

func TestNormalise_emptyLevel_defaultsToINFO(t *testing.T) {
	p := newProcessor(&mockProducer{}, nil)
	entry := models.LogEntry{TenantID: "t1", Message: "hello", Level: ""}
	got, ok := p.normalise(entry)
	if !ok {
		t.Fatal("expected normalise to succeed")
	}
	if got.Level != "INFO" {
		t.Errorf("empty level should default to INFO, got %q", got.Level)
	}
}

func TestNormalise_levelUppercased(t *testing.T) {
	p := newProcessor(&mockProducer{}, nil)
	tests := []struct{ in, out string }{
		{"error", "ERROR"},
		{"warn", "WARN"},
		{"Debug", "DEBUG"},
		{"FATAL", "FATAL"},
	}
	for _, tc := range tests {
		entry := models.LogEntry{TenantID: "t1", Message: "msg", Level: tc.in}
		got, ok := p.normalise(entry)
		if !ok {
			t.Errorf("normalise failed for level %q", tc.in)
			continue
		}
		if got.Level != tc.out {
			t.Errorf("level %q: got %q, want %q", tc.in, got.Level, tc.out)
		}
	}
}

func TestNormalise_zeroEventTimestamp_setFromIngest(t *testing.T) {
	p := newProcessor(&mockProducer{}, nil)
	ingest := time.Now().UTC()
	entry := models.LogEntry{
		TenantID:        "t1",
		Message:         "msg",
		Level:           "INFO",
		IngestTimestamp: ingest,
		// EventTimestamp deliberately zero
	}
	got, ok := p.normalise(entry)
	if !ok {
		t.Fatal("expected success")
	}
	if got.EventTimestamp.IsZero() {
		t.Error("EventTimestamp should be set from IngestTimestamp when zero")
	}
	if !got.EventTimestamp.Equal(ingest) {
		t.Errorf("EventTimestamp: got %v, want %v", got.EventTimestamp, ingest)
	}
}

func TestNormalise_existingEventTimestamp_preserved(t *testing.T) {
	p := newProcessor(&mockProducer{}, nil)
	evtTime := time.Now().UTC().Add(-5 * time.Minute)
	entry := models.LogEntry{
		TenantID:       "t1",
		Message:        "msg",
		Level:          "INFO",
		EventTimestamp: evtTime,
	}
	got, ok := p.normalise(entry)
	if !ok {
		t.Fatal("expected success")
	}
	if !got.EventTimestamp.Equal(evtTime) {
		t.Errorf("EventTimestamp should not be overwritten: got %v", got.EventTimestamp)
	}
}

// ── sendDLQ ───────────────────────────────────────────────────────────────────

func TestSendDLQ_publishesToDLQTopic(t *testing.T) {
	prod := &mockProducer{}
	p := newProcessor(prod, nil)
	msg := &kafka.Message{Value: []byte(`{"test":"data"}`)}
	p.sendDLQ(context.Background(), msg, "test reason")

	msgs := prod.messages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 DLQ message, got %d", len(msgs))
	}
	if msgs[0].Topic != kafka.TopicDeadLetter {
		t.Errorf("DLQ topic: got %q, want %q", msgs[0].Topic, kafka.TopicDeadLetter)
	}
}

func TestSendDLQ_payloadContainsReason(t *testing.T) {
	prod := &mockProducer{}
	p := newProcessor(prod, nil)
	msg := &kafka.Message{Value: []byte(`{"original":"payload"}`)}
	p.sendDLQ(context.Background(), msg, "unmarshal error")

	msgs := prod.messages()
	if len(msgs) == 0 {
		t.Fatal("no DLQ messages published")
	}
	var envelope struct {
		Reason  string          `json:"reason"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(msgs[0].Value, &envelope); err != nil {
		t.Fatalf("decode DLQ envelope: %v", err)
	}
	if envelope.Reason != "unmarshal error" {
		t.Errorf("reason: got %q, want %q", envelope.Reason, "unmarshal error")
	}
}

// ── handle ────────────────────────────────────────────────────────────────────

func TestHandle_validBatch_publishesNormalized(t *testing.T) {
	prod := &mockProducer{}
	p := newProcessor(prod, nil)

	batch := models.LogBatch{
		Entries: []models.LogEntry{
			{TenantID: "t1", Service: "svc", Level: "error", Message: "boot"},
		},
	}
	msg := batchMsg(batch)
	p.handle(context.Background(), msg)

	msgs := prod.messages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 normalized message, got %d", len(msgs))
	}
	if msgs[0].Topic != kafka.TopicLogsNormalized {
		t.Errorf("topic: got %q, want %q", msgs[0].Topic, kafka.TopicLogsNormalized)
	}
}

func TestHandle_invalidJSON_sendsToDLQ(t *testing.T) {
	prod := &mockProducer{}
	p := newProcessor(prod, nil)

	msg := &kafka.Message{Value: []byte("not-json")}
	p.handle(context.Background(), msg)

	msgs := prod.messages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 DLQ message for invalid JSON, got %d", len(msgs))
	}
	if msgs[0].Topic != kafka.TopicDeadLetter {
		t.Errorf("expected DLQ topic, got %q", msgs[0].Topic)
	}
}

func TestHandle_normaliseFails_sendsToDLQ(t *testing.T) {
	prod := &mockProducer{}
	p := newProcessor(prod, nil)

	// Entry with no TenantID → normalise fails → DLQ.
	batch := models.LogBatch{
		Entries: []models.LogEntry{
			{TenantID: "", Service: "svc", Message: "no tenant"},
		},
	}
	msg := batchMsg(batch)
	p.handle(context.Background(), msg)

	msgs := prod.messages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 DLQ message for normalise failure, got %d", len(msgs))
	}
	if msgs[0].Topic != kafka.TopicDeadLetter {
		t.Errorf("expected DLQ, got %q", msgs[0].Topic)
	}
}

func TestHandle_dedupSkipsDuplicate(t *testing.T) {
	prod := &mockProducer{}
	dedup := newMockDedup()
	p := newProcessor(prod, dedup)

	entry := models.LogEntry{TenantID: "t1", Service: "svc", LogID: "dup-id", Level: "INFO", Message: "first"}
	batch := models.LogBatch{Entries: []models.LogEntry{entry}}
	msg := batchMsg(batch)

	// First handle → new, should publish.
	p.handle(context.Background(), msg)
	// Second handle → duplicate, should NOT publish again.
	p.handle(context.Background(), msg)

	if len(prod.messages()) != 1 {
		t.Errorf("expected 1 published message (duplicate skipped), got %d", len(prod.messages()))
	}
}

func TestHandle_dedupAllowsUniqueEntries(t *testing.T) {
	prod := &mockProducer{}
	dedup := newMockDedup()
	p := newProcessor(prod, dedup)

	for i := 0; i < 3; i++ {
		batch := models.LogBatch{
			Entries: []models.LogEntry{
				{TenantID: "t1", Service: "svc", LogID: uniqueLogID(i), Level: "INFO", Message: "msg"},
			},
		}
		p.handle(context.Background(), batchMsg(batch))
	}

	if len(prod.messages()) != 3 {
		t.Errorf("expected 3 published messages for 3 unique entries, got %d", len(prod.messages()))
	}
}

func TestHandle_multipleBatchEntries(t *testing.T) {
	prod := &mockProducer{}
	p := newProcessor(prod, nil)

	batch := models.LogBatch{
		Entries: []models.LogEntry{
			{TenantID: "t1", Service: "svc", Level: "INFO", Message: "msg1"},
			{TenantID: "t1", Service: "svc", Level: "ERROR", Message: "msg2"},
			{TenantID: "t1", Service: "svc", Level: "WARN", Message: "msg3"},
		},
	}
	p.handle(context.Background(), batchMsg(batch))

	if len(prod.messages()) != 3 {
		t.Errorf("expected 3 normalized messages, got %d", len(prod.messages()))
	}
}

func TestHandle_commitsMessage(t *testing.T) {
	consumer := newMockConsumer()
	prod := &mockProducer{}
	cfg := Config{ConsumerGroup: "g", DLQTopic: kafka.TopicDeadLetter}
	p := New(cfg, consumer, prod, nil, nil, nil)

	batch := models.LogBatch{
		Entries: []models.LogEntry{
			{TenantID: "t1", Service: "svc", Level: "INFO", Message: "hi"},
		},
	}
	msg := batchMsg(batch)
	p.handle(context.Background(), msg)

	consumer.mu.Lock()
	committed := len(consumer.committed)
	consumer.mu.Unlock()

	if committed != 1 {
		t.Errorf("expected message to be committed, committed=%d", committed)
	}
}

// ── Start / Stop ──────────────────────────────────────────────────────────────

func TestProcessor_StartStop(t *testing.T) {
	p := newProcessor(&mockProducer{}, nil)
	if err := p.Start(); err != nil {
		t.Fatalf("Start() error: %v", err)
	}
	// Stop should not block or panic.
	p.Stop()
}

// ── uniqueLogID helper ────────────────────────────────────────────────────────

func uniqueLogID(n int) string {
	return "id-" + string(rune('a'+n))
}

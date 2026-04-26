package processor

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/distributed-logging/shared/kafka"
	"github.com/distributed-logging/shared/models"
)

// Config holds stream processor settings.
type Config struct {
	ConsumerGroup string
	DLQTopic      string
}

// Processor reads raw log batches from Kafka, normalises them, and emits
// normalised entries to logs-normalized. Invalid entries go to the DLQ.
type Processor struct {
	cfg      Config
	consumer kafka.Consumer
	producer kafka.Producer
	done     chan struct{}
}

// New creates a Processor.
func New(cfg Config, consumer kafka.Consumer, producer kafka.Producer) *Processor {
	return &Processor{
		cfg:      cfg,
		consumer: consumer,
		producer: producer,
		done:     make(chan struct{}),
	}
}

// Start subscribes and begins the processing loop.
func (p *Processor) Start() error {
	if err := p.consumer.Subscribe([]string{kafka.TopicLogsRaw}); err != nil {
		return err
	}
	go p.loop()
	return nil
}

// Stop signals the processor to stop.
func (p *Processor) Stop() { close(p.done) }

func (p *Processor) loop() {
	ctx := context.Background()
	for {
		select {
		case <-p.done:
			return
		default:
		}
		msg, err := p.consumer.Poll(ctx)
		if err != nil {
			log.Printf("processor: poll: %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		p.handle(ctx, msg)
	}
}

func (p *Processor) handle(ctx context.Context, msg *kafka.Message) {
	var batch models.LogBatch
	if err := json.Unmarshal(msg.Value, &batch); err != nil {
		p.sendDLQ(ctx, msg, "unmarshal error: "+err.Error())
		p.consumer.Commit(ctx, msg)
		return
	}

	for _, entry := range batch.Entries {
		normalised, ok := p.normalise(entry)
		if !ok {
			raw, _ := json.Marshal(entry)
			p.sendDLQ(ctx, &kafka.Message{Value: raw}, "normalise failed")
			continue
		}
		out := kafka.Message{
			Topic:     kafka.TopicLogsNormalized,
			Key:       kafka.LogPartitionKey(normalised.TenantID, normalised.Service),
			Timestamp: normalised.IngestTimestamp,
		}
		if err := kafka.MarshalValue(&out, normalised); err != nil {
			log.Printf("processor: marshal normalised: %v", err)
			continue
		}
		if err := p.producer.Publish(ctx, out); err != nil {
			log.Printf("processor: publish: %v", err)
		}
	}
	p.consumer.Commit(ctx, msg)
}

// normalise validates and enriches a single log entry.
func (p *Processor) normalise(e models.LogEntry) (models.LogEntry, bool) {
	if e.TenantID == "" || e.Message == "" {
		return e, false
	}
	e.Level = strings.ToUpper(strings.TrimSpace(e.Level))
	if e.Level == "" {
		e.Level = "INFO"
	}
	if e.EventTimestamp.IsZero() {
		e.EventTimestamp = e.IngestTimestamp
	}
	return e, true
}

func (p *Processor) sendDLQ(ctx context.Context, msg *kafka.Message, reason string) {
	type dlqEnvelope struct {
		Reason  string          `json:"reason"`
		Payload json.RawMessage `json:"payload"`
	}
	env := dlqEnvelope{Reason: reason, Payload: msg.Value}
	out := kafka.Message{Topic: p.cfg.DLQTopic}
	if err := kafka.MarshalValue(&out, env); err != nil {
		return
	}
	if err := p.producer.Publish(ctx, out); err != nil {
		log.Printf("processor: DLQ publish: %v", err)
	}
}

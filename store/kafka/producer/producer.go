package producer

import (
	"context"
	"fmt"
	"strings"

	"github.com/hossainshakhawat/distributed-logging/store-kafka/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Config holds Kafka producer settings.
type Config struct {
	// Brokers is a comma-separated list or a slice of broker addresses.
	Brokers []string
}

// FranzProducer implements kafka.Producer using franz-go.
type FranzProducer struct {
	client *kgo.Client
}

// New creates a connected FranzProducer.
func New(cfg Config) (*FranzProducer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka producer: at least one broker is required")
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()), // wait for all in-sync replicas
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	return &FranzProducer{client: cl}, nil
}

// NewFromEnv builds a FranzProducer from a comma-separated KAFKA_BROKERS env string.
func NewFromEnv(brokerList string) (*FranzProducer, error) {
	return New(Config{Brokers: splitBrokers(brokerList)})
}

// Publish synchronously sends a single message and returns when it is acknowledged.
func (p *FranzProducer) Publish(ctx context.Context, msg kafka.Message) error {
	record := &kgo.Record{
		Topic:     msg.Topic,
		Key:       msg.Key,
		Value:     msg.Value,
		Timestamp: msg.Timestamp,
	}
	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("kafka produce to %s: %w", msg.Topic, err)
	}
	return nil
}

// Close flushes pending messages and closes the connection.
func (p *FranzProducer) Close() error {
	p.client.Close()
	return nil
}

func splitBrokers(s string) []string {
	var out []string
	for _, b := range strings.Split(s, ",") {
		b = strings.TrimSpace(b)
		if b != "" {
			out = append(out, b)
		}
	}
	return out
}

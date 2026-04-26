package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// Message is a generic Kafka message envelope.
type Message struct {
	Topic     string
	Key       []byte
	Value     []byte
	Timestamp time.Time
}

// Producer is a minimal interface for publishing messages.
type Producer interface {
	Publish(ctx context.Context, msg Message) error
	Close() error
}

// Consumer is a minimal interface for consuming messages.
type Consumer interface {
	Subscribe(topics []string) error
	Poll(ctx context.Context) (*Message, error)
	Commit(ctx context.Context, msg *Message) error
	Close() error
}

// MarshalValue serialises v to JSON and stores it in msg.Value.
func MarshalValue(msg *Message, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("kafka marshal: %w", err)
	}
	msg.Value = b
	return nil
}

// UnmarshalValue deserialises msg.Value from JSON into v.
func UnmarshalValue(msg *Message, v any) error {
	if err := json.Unmarshal(msg.Value, v); err != nil {
		return fmt.Errorf("kafka unmarshal: %w", err)
	}
	return nil
}

// Topics used across the system.
const (
	TopicLogsRaw        = "logs-raw"
	TopicLogsNormalized = "logs-normalized"
	TopicDeadLetter     = "logs-dead-letter"
)

// LogPartitionKey returns the Kafka partition key for a tenant+service pair.
func LogPartitionKey(tenantID, serviceName string) []byte {
	return []byte(tenantID + "|" + serviceName)
}

// StubProducer is a simple stdout producer used for local dev / tests.
type StubProducer struct{}

func (s *StubProducer) Publish(_ context.Context, msg Message) error {
	log.Printf("[KAFKA STUB] topic=%s key=%s value=%s", msg.Topic, msg.Key, msg.Value)
	return nil
}
func (s *StubProducer) Close() error { return nil }

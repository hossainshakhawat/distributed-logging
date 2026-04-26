package consumer

import (
	"context"
	"fmt"
	"strings"

	"github.com/distributed-logging/shared/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Config holds Kafka consumer settings.
type Config struct {
	Brokers       []string
	ConsumerGroup string
}

// FranzConsumer implements kafka.Consumer using franz-go.
// Call Subscribe before Poll.
type FranzConsumer struct {
	cfg    Config
	client *kgo.Client
	ch     chan *kgo.Record
	done   chan struct{}
}

// New creates a FranzConsumer. Subscribe must be called to start receiving.
func New(cfg Config) *FranzConsumer {
	return &FranzConsumer{
		cfg:  cfg,
		ch:   make(chan *kgo.Record, 256),
		done: make(chan struct{}),
	}
}

// NewFromEnv builds a FranzConsumer from a comma-separated KAFKA_BROKERS env string.
func NewFromEnv(brokerList, group string) *FranzConsumer {
	return New(Config{
		Brokers:       splitBrokers(brokerList),
		ConsumerGroup: group,
	})
}

// Subscribe creates the underlying Kafka client and begins background polling.
func (c *FranzConsumer) Subscribe(topics []string) error {
	if len(c.cfg.Brokers) == 0 {
		return fmt.Errorf("kafka consumer: at least one broker is required")
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.cfg.Brokers...),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumerGroup(c.cfg.ConsumerGroup),
		kgo.DisableAutoCommit(),
		kgo.FetchMinBytes(1),
	)
	if err != nil {
		return fmt.Errorf("kafka consumer: %w", err)
	}
	c.client = cl
	go c.pollLoop()
	return nil
}

// Poll returns the next available message, blocking until one arrives or ctx is cancelled.
func (c *FranzConsumer) Poll(ctx context.Context) (*kafka.Message, error) {
	select {
	case r, ok := <-c.ch:
		if !ok {
			return nil, fmt.Errorf("consumer closed")
		}
		return &kafka.Message{
			Topic:     r.Topic,
			Key:       r.Key,
			Value:     r.Value,
			Timestamp: r.Timestamp,
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Commit marks all polled-but-uncommitted offsets as processed.
func (c *FranzConsumer) Commit(ctx context.Context, _ *kafka.Message) error {
	if c.client == nil {
		return nil
	}
	return c.client.CommitUncommittedOffsets(ctx)
}

// Close stops the background poller and closes the Kafka connection.
func (c *FranzConsumer) Close() error {
	select {
	case <-c.done:
	default:
		close(c.done)
	}
	if c.client != nil {
		c.client.Close()
	}
	return nil
}

// pollLoop fetches records from Kafka and sends them to the internal channel.
func (c *FranzConsumer) pollLoop() {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-c.done
		cancel()
	}()
	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		fetches.EachRecord(func(r *kgo.Record) {
			select {
			case c.ch <- r:
			case <-c.done:
			}
		})
	}
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

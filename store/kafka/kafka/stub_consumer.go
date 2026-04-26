package kafka

import (
	"context"
	"log"
)

// StubConsumer is a no-op consumer for dev/testing.
type StubConsumer struct{}

func (s *StubConsumer) Subscribe(_ []string) error { return nil }
func (s *StubConsumer) Poll(_ context.Context) (*Message, error) {
	// Block forever in stub — real impl reads from Kafka
	select {}
}
func (s *StubConsumer) Commit(_ context.Context, _ *Message) error { return nil }
func (s *StubConsumer) Close() error {
	log.Println("StubConsumer closed")
	return nil
}

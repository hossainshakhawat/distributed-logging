package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/distributed-logging/shared/config"
	"github.com/distributed-logging/shared/kafka"
	"github.com/distributed-logging/stream-processor/internal/processor"
)

func main() {
	consumer := &kafka.StubConsumer{} // swap with real Kafka consumer
	producer := &kafka.StubProducer{} // publishes to logs-normalized

	cfg := processor.Config{
		ConsumerGroup: config.Getenv("CONSUMER_GROUP", "stream-processor"),
		DLQTopic:      kafka.TopicDeadLetter,
	}

	p := processor.New(cfg, consumer, producer)
	if err := p.Start(); err != nil {
		log.Fatalf("processor start: %v", err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	p.Stop()
}

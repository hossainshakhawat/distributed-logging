package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/distributed-logging/shared/config"
	sharedkafka "github.com/distributed-logging/shared/kafka"
	kafkaconsumer "github.com/distributed-logging/store-kafka/consumer"
	kafkaproducer "github.com/distributed-logging/store-kafka/producer"
	osclient "github.com/distributed-logging/store-opensearch/client"
	redisclient "github.com/distributed-logging/store-redis/client"
	s3client "github.com/distributed-logging/store-s3/client"
	"github.com/distributed-logging/stream-processor/internal/archiver"
	"github.com/distributed-logging/stream-processor/internal/indexer"
	"github.com/distributed-logging/stream-processor/internal/processor"
)

func main() {
	brokers := config.Getenv("KAFKA_BROKERS", "localhost:9092")

	// ── Kafka ──────────────────────────────────────────────────────────────
	consumer := kafkaconsumer.NewFromEnv(brokers,
		config.Getenv("CONSUMER_GROUP", "stream-processor"))
	prod, err := kafkaproducer.NewFromEnv(brokers)
	if err != nil {
		log.Fatalf("kafka producer: %v", err)
	}
	defer prod.Close()

	// ── OpenSearch indexer ─────────────────────────────────────────────────
	osClient, err := osclient.New(osclient.Config{
		Addresses: []string{config.Getenv("OPENSEARCH_ADDR", "http://localhost:9200")},
		Username:  config.Getenv("OPENSEARCH_USER", "admin"),
		Password:  config.Getenv("OPENSEARCH_PASS", "Admin@12345"),
	})
	if err != nil {
		log.Fatalf("opensearch client: %v", err)
	}
	idx := indexer.New(osClient, 500, 5*time.Second)
	defer idx.Stop()

	// ── S3 archiver ────────────────────────────────────────────────────────
	s3Cfg := s3client.Config{
		Bucket:   config.Getenv("S3_BUCKET", "distributed-logs"),
		Region:   config.Getenv("AWS_REGION", "us-east-1"),
		Endpoint: config.Getenv("S3_ENDPOINT", ""), // empty = real AWS
	}
	s3c, err := s3client.New(context.Background(), s3Cfg)
	if err != nil {
		log.Fatalf("s3 client: %v", err)
	}
	arch := archiver.New(s3c, 1000, 30*time.Second)
	defer arch.Stop()

	// ── Redis dedup cache ──────────────────────────────────────────────────
	redisClient := redisclient.New(redisclient.Config{
		Addr:     config.Getenv("REDIS_ADDR", "localhost:6379"),
		Password: config.Getenv("REDIS_PASS", ""),
	})
	defer redisClient.Close()

	// ── Normalizer pipeline ────────────────────────────────────────────────
	cfg := processor.Config{
		ConsumerGroup: config.Getenv("CONSUMER_GROUP", "stream-processor"),
		DLQTopic:      sharedkafka.TopicDeadLetter,
	}
	p := processor.New(cfg, consumer, prod, idx, arch, redisClient)
	if err := p.Start(); err != nil {
		log.Fatalf("processor start: %v", err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	p.Stop()
}

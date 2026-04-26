package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	sharedkafka "github.com/hossainshakhawat/distributed-logging/store-kafka/kafka"
	kafkaconsumer "github.com/hossainshakhawat/distributed-logging/store-kafka/consumer"
	kafkaproducer "github.com/hossainshakhawat/distributed-logging/store-kafka/producer"
	osclient "github.com/hossainshakhawat/distributed-logging/store-opensearch/client"
	redisclient "github.com/hossainshakhawat/distributed-logging/store-redis/client"
	s3client "github.com/hossainshakhawat/distributed-logging/store-s3/client"
	spconfig "github.com/hossainshakhawat/distributed-logging/stream-processor/config"
	"github.com/hossainshakhawat/distributed-logging/stream-processor/internal/archiver"
	"github.com/hossainshakhawat/distributed-logging/stream-processor/internal/indexer"
	"github.com/hossainshakhawat/distributed-logging/stream-processor/internal/processor"
)

func main() {
	cfg, err := spconfig.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	// ── Kafka ──────────────────────────────────────────────────────────────
	consumer := kafkaconsumer.New(kafkaconsumer.Config{
		Brokers:       cfg.Kafka.Brokers,
		ConsumerGroup: cfg.Kafka.ConsumerGroup,
	})
	prod, err := kafkaproducer.New(kafkaproducer.Config{
		Brokers: cfg.Kafka.Brokers,
	})
	if err != nil {
		log.Fatalf("kafka producer: %v", err)
	}
	defer prod.Close()

	// ── OpenSearch indexer ─────────────────────────────────────────────────
	osClient, err := osclient.New(osclient.Config{
		Addresses: cfg.OpenSearch.Addresses,
		Username:  cfg.OpenSearch.Username,
		Password:  cfg.OpenSearch.Password,
	})
	if err != nil {
		log.Fatalf("opensearch client: %v", err)
	}
	idx := indexer.New(
		osClient,
		cfg.Indexer.BatchSize,
		time.Duration(cfg.Indexer.FlushIntervalSecs)*time.Second,
	)
	defer idx.Stop()

	// ── S3 archiver ────────────────────────────────────────────────────────
	s3c, err := s3client.New(context.Background(), s3client.Config{
		Bucket:   cfg.S3.Bucket,
		Region:   cfg.S3.Region,
		Endpoint: cfg.S3.Endpoint,
	})
	if err != nil {
		log.Fatalf("s3 client: %v", err)
	}
	arch := archiver.New(
		s3c,
		cfg.Archiver.BatchSize,
		time.Duration(cfg.Archiver.FlushIntervalSecs)*time.Second,
	)
	defer arch.Stop()

	// ── Redis dedup cache ──────────────────────────────────────────────────
	redisClient := redisclient.New(redisclient.Config{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	defer redisClient.Close()

	// ── Normalizer pipeline ────────────────────────────────────────────────
	procCfg := processor.Config{
		ConsumerGroup: cfg.Kafka.ConsumerGroup,
		DLQTopic:      sharedkafka.TopicDeadLetter,
		DedupTTL:      time.Duration(cfg.DedupTTLSecs) * time.Second,
	}
	p := processor.New(procCfg, consumer, prod, idx, arch, redisClient)
	if err := p.Start(); err != nil {
		log.Fatalf("processor start: %v", err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	p.Stop()
}

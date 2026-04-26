package main

import (
	"log"
	"net/http"

	"github.com/distributed-logging/ingestion-gateway/internal/gateway"
	"github.com/distributed-logging/shared/config"
	kafkaproducer "github.com/distributed-logging/store-kafka/producer"
)

func main() {
	brokers := config.Getenv("KAFKA_BROKERS", "localhost:9092")
	producer, err := kafkaproducer.NewFromEnv(brokers)
	if err != nil {
		log.Fatalf("kafka producer: %v", err)
	}
	defer producer.Close()

	cfg := gateway.Config{
		ListenAddr:   config.Getenv("LISTEN_ADDR", ":8080"),
		RateLimitRPS: 1000,
		ValidAPIKeys: map[string]string{"tenant-a": "secret-a", "tenant-b": "secret-b"},
	}

	srv := gateway.NewServer(cfg, producer)
	log.Printf("ingestion-gateway listening on %s", cfg.ListenAddr)
	if err := http.ListenAndServe(cfg.ListenAddr, srv); err != nil {
		log.Fatalf("server: %v", err)
	}
}

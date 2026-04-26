package main

import (
	"log"
	"net/http"

	"github.com/distributed-logging/shared/config"
	kafkaconsumer "github.com/distributed-logging/store-kafka/consumer"
	"github.com/distributed-logging/tail-service/internal/tail"
)

func main() {
	brokers := config.Getenv("KAFKA_BROKERS", "localhost:9092")
	consumer := kafkaconsumer.NewFromEnv(brokers, "tail-service")

	cfg := tail.Config{
		ListenAddr:        config.Getenv("LISTEN_ADDR", ":8082"),
		MaxActiveSessions: 500,
	}

	srv := tail.NewServer(cfg, consumer)
	log.Printf("tail-service listening on %s", cfg.ListenAddr)
	if err := http.ListenAndServe(cfg.ListenAddr, srv); err != nil {
		log.Fatalf("server: %v", err)
	}
}

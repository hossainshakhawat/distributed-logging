package main

import (
	"log"
	"net/http"

	"github.com/distributed-logging/shared/config"
	"github.com/distributed-logging/shared/kafka"
	"github.com/distributed-logging/tail-service/internal/tail"
)

func main() {
	consumer := &kafka.StubConsumer{}

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

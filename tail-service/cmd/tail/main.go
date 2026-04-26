package main

import (
	"log"
	"net/http"

	tsconfig "github.com/distributed-logging/tail-service/config"
	"github.com/distributed-logging/tail-service/internal/tail"
	kafkaconsumer "github.com/distributed-logging/store-kafka/consumer"
)

func main() {
	cfg, err := tsconfig.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	consumer := kafkaconsumer.New(kafkaconsumer.Config{
		Brokers:       cfg.Kafka.Brokers,
		ConsumerGroup: cfg.Kafka.ConsumerGroup,
	})

	srvCfg := tail.Config{
		ListenAddr:        cfg.ListenAddr,
		MaxActiveSessions: cfg.MaxActiveSessions,
	}

	srv := tail.NewServer(srvCfg, consumer)
	log.Printf("tail-service listening on %s", cfg.ListenAddr)
	if err := http.ListenAndServe(cfg.ListenAddr, srv); err != nil {
		log.Fatalf("server: %v", err)
	}
}

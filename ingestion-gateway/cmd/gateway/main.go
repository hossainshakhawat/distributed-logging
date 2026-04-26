package main

import (
	"log"
	"net/http"

	gwconfig "github.com/distributed-logging/ingestion-gateway/config"
	"github.com/distributed-logging/ingestion-gateway/internal/gateway"
	kafkaproducer "github.com/distributed-logging/store-kafka/producer"
)

func main() {
	cfg, err := gwconfig.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	producer, err := kafkaproducer.New(kafkaproducer.Config{
		Brokers: cfg.Kafka.Brokers,
	})
	if err != nil {
		log.Fatalf("kafka producer: %v", err)
	}
	defer producer.Close()

	srvCfg := gateway.Config{
		ListenAddr:   cfg.ListenAddr,
		RateLimitRPS: cfg.RateLimitRPS,
		ValidAPIKeys: cfg.APIKeys,
	}

	srv := gateway.NewServer(srvCfg, producer)
	log.Printf("ingestion-gateway listening on %s", cfg.ListenAddr)
	if err := http.ListenAndServe(cfg.ListenAddr, srv); err != nil {
		log.Fatalf("server: %v", err)
	}
}

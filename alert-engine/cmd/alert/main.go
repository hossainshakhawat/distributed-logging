package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	aeconfig "github.com/hossainshakhawat/distributed-logging/alert-engine/config"
	"github.com/hossainshakhawat/distributed-logging/alert-engine/internal/alert"
	kafkaconsumer "github.com/hossainshakhawat/distributed-logging/store-kafka/consumer"
)

func main() {
	cfg, err := aeconfig.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	consumer := kafkaconsumer.New(kafkaconsumer.Config{
		Brokers:       cfg.Kafka.Brokers,
		ConsumerGroup: cfg.Kafka.ConsumerGroup,
	})

	rules := make([]alert.Rule, len(cfg.Rules))
	for i, r := range cfg.Rules {
		rules[i] = alert.Rule{
			Name:            r.Name,
			TenantID:        r.TenantID,
			Level:           r.Level,
			MessageContains: r.MessageContains,
			Threshold:       r.Threshold,
			WindowSecs:      r.WindowSecs,
			Webhook:         r.Webhook,
		}
	}

	engine := alert.NewEngine(consumer, rules, cfg.Kafka.Topics.LogsNormalized)
	if err := engine.Start(); err != nil {
		log.Fatalf("alert engine start: %v", err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	engine.Stop()
}

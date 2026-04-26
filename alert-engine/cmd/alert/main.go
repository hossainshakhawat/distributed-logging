package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/distributed-logging/alert-engine/internal/alert"
	"github.com/distributed-logging/shared/kafka"
)

func main() {
	consumer := &kafka.StubConsumer{}

	rules := []alert.Rule{
		{
			Name:        "high-error-rate",
			TenantID:    "*",
			Level:       "ERROR",
			Threshold:   100,
			WindowSecs:  300,
			Webhook:     "http://localhost:9000/alerts",
		},
		{
			Name:        "payment-failed",
			TenantID:    "*",
			MessageContains: "payment failed",
			Threshold:   1,
			WindowSecs:  60,
			Webhook:     "http://localhost:9000/alerts",
		},
	}

	engine := alert.NewEngine(consumer, rules)
	if err := engine.Start(); err != nil {
		log.Fatalf("alert engine start: %v", err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	engine.Stop()
}

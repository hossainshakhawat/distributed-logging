package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/distributed-logging/log-agent/internal/agent"
	"github.com/distributed-logging/shared/config"
)

func main() {
	cfg := agent.Config{
		GatewayURL:    config.Getenv("GATEWAY_URL", "http://localhost:8080"),
		TenantID:      config.Getenv("TENANT_ID", "default"),
		Service:       config.Getenv("SERVICE_NAME", "unknown"),
		Host:          mustHostname(),
		Environment:   config.Getenv("ENVIRONMENT", "production"),
		WatchPaths:    []string{config.Getenv("LOG_PATH", "/var/log/app/app.log")},
		BatchSize:     100,
		FlushInterval: 2, // seconds
		BufferDir:     config.Getenv("BUFFER_DIR", "/tmp/log-agent-buffer"),
	}

	a := agent.New(cfg)
	if err := a.Start(); err != nil {
		log.Fatalf("agent start: %v", err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	a.Stop()
}

func mustHostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown-host"
	}
	return h
}

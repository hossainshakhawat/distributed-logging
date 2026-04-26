package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	agentconfig "github.com/distributed-logging/log-agent/config"
	"github.com/distributed-logging/log-agent/internal/agent"
)

func main() {
	cfg, err := agentconfig.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	agentCfg := agent.Config{
		GatewayURL:    cfg.GatewayURL,
		TenantID:      cfg.TenantID,
		Service:       cfg.ServiceName,
		Host:          mustHostname(),
		Environment:   cfg.Environment,
		WatchPaths:    cfg.LogPaths,
		BatchSize:     cfg.BatchSize,
		FlushInterval: cfg.FlushIntervalSecs,
		BufferDir:     cfg.BufferDir,
	}

	a := agent.New(agentCfg)
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

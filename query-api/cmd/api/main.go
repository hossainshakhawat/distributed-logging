package main

import (
	"log"
	"net/http"

	"github.com/distributed-logging/query-api/internal/api"
	"github.com/distributed-logging/shared/config"
)

func main() {
	cfg := api.Config{
		ListenAddr:       config.Getenv("LISTEN_ADDR", ":8081"),
		OpenSearchAddr:   config.Getenv("OPENSEARCH_ADDR", "http://localhost:9200"),
		HotRetentionDays: 30,
		ValidAPIKeys:     map[string]string{"tenant-a": "secret-a"},
	}

	srv := api.NewServer(cfg)
	log.Printf("query-api listening on %s", cfg.ListenAddr)
	if err := http.ListenAndServe(cfg.ListenAddr, srv); err != nil {
		log.Fatalf("server: %v", err)
	}
}

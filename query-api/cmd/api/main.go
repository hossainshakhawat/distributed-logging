package main

import (
	"log"
	"net/http"
	"strings"

	"github.com/distributed-logging/query-api/internal/api"
	"github.com/distributed-logging/shared/config"
	osclient "github.com/distributed-logging/store-opensearch/client"
)

func main() {
	osClient, err := osclient.New(osclient.Config{
		Addresses: strings.Split(config.Getenv("OPENSEARCH_ADDR", "http://localhost:9200"), ","),
		Username:  config.Getenv("OPENSEARCH_USER", "admin"),
		Password:  config.Getenv("OPENSEARCH_PASS", "Admin@12345"),
	})
	if err != nil {
		log.Fatalf("opensearch client: %v", err)
	}

	cfg := api.Config{
		ListenAddr:       config.Getenv("LISTEN_ADDR", ":8081"),
		HotRetentionDays: 30,
		ValidAPIKeys:     map[string]string{"tenant-a": "secret-a"},
	}

	srv := api.NewServer(cfg, osClient)
	log.Printf("query-api listening on %s", cfg.ListenAddr)
	if err := http.ListenAndServe(cfg.ListenAddr, srv); err != nil {
		log.Fatalf("server: %v", err)
	}
}

package main

import (
	"log"
	"net/http"

	qaconfig "github.com/distributed-logging/query-api/config"
	"github.com/distributed-logging/query-api/internal/api"
	osclient "github.com/distributed-logging/store-opensearch/client"
)

func main() {
	cfg, err := qaconfig.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	osClient, err := osclient.New(osclient.Config{
		Addresses: cfg.OpenSearch.Addresses,
		Username:  cfg.OpenSearch.Username,
		Password:  cfg.OpenSearch.Password,
	})
	if err != nil {
		log.Fatalf("opensearch client: %v", err)
	}

	srvCfg := api.Config{
		ListenAddr:       cfg.ListenAddr,
		HotRetentionDays: cfg.HotRetentionDays,
		ValidAPIKeys:     cfg.APIKeys,
	}

	srv := api.NewServer(srvCfg, osClient)
	log.Printf("query-api listening on %s", cfg.ListenAddr)
	if err := http.ListenAndServe(cfg.ListenAddr, srv); err != nil {
		log.Fatalf("server: %v", err)
	}
}

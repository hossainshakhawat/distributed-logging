package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/distributed-logging/shared/models"
	opensearch "github.com/opensearch-project/opensearch-go/v2"
	"github.com/opensearch-project/opensearch-go/v2/opensearchapi"
)

// Config holds OpenSearch client settings.
type Config struct {
	Addresses []string
	Username  string
	Password  string
}

// Client wraps the OpenSearch client with log-domain operations.
type Client struct {
	os *opensearch.Client
}

// New creates a Client connected to OpenSearch.
func New(cfg Config) (*Client, error) {
	cl, err := opensearch.NewClient(opensearch.Config{
		Addresses: cfg.Addresses,
		Username:  cfg.Username,
		Password:  cfg.Password,
	})
	if err != nil {
		return nil, fmt.Errorf("opensearch client: %w", err)
	}
	return &Client{os: cl}, nil
}

// IndexName returns the daily index name for a tenant.
// Pattern: logs-{tenant_id}-{yyyy-mm-dd}
func IndexName(tenantID string, t time.Time) string {
	return fmt.Sprintf("logs-%s-%s", tenantID, t.UTC().Format("2006-01-02"))
}

// IndexBulk indexes a batch of log entries using the Bulk API for efficiency.
// Each entry is indexed with its LogID as the document ID (idempotent upsert).
func (c *Client) IndexBulk(ctx context.Context, entries []models.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	var buf bytes.Buffer
	for _, e := range entries {
		// Action line
		meta := map[string]any{
			"index": map[string]any{
				"_index": IndexName(e.TenantID, e.EventTimestamp),
				"_id":    e.LogID,
			},
		}
		if err := json.NewEncoder(&buf).Encode(meta); err != nil {
			return err
		}
		// Document line
		if err := json.NewEncoder(&buf).Encode(e); err != nil {
			return err
		}
	}
	req := opensearchapi.BulkRequest{Body: &buf}
	resp, err := req.Do(ctx, c.os)
	if err != nil {
		return fmt.Errorf("opensearch bulk: %w", err)
	}
	defer resp.Body.Close()
	if resp.IsError() {
		return fmt.Errorf("opensearch bulk error: %s", resp.Status())
	}
	return nil
}

// SearchQuery holds all parameters for a log search.
type SearchQuery struct {
	TenantID string
	Service  string
	Level    string
	Query    string // full-text on message field
	From     time.Time
	To       time.Time
	Page     int
	PageSize int
}

// SearchResult is the response from a Search call.
type SearchResult struct {
	Total   int
	Entries []models.LogEntry
}

// Search executes a DSL query against the relevant daily indices.
func (c *Client) Search(ctx context.Context, q SearchQuery) (SearchResult, error) {
	// Build bool/must filter clauses
	must := []map[string]any{
		{"term": map[string]any{"tenant_id": q.TenantID}},
		{"range": map[string]any{
			"event_timestamp": map[string]any{
				"gte": q.From.UTC().Format(time.RFC3339),
				"lte": q.To.UTC().Format(time.RFC3339),
			},
		}},
	}
	if q.Service != "" {
		must = append(must, map[string]any{"term": map[string]any{"service": q.Service}})
	}
	if q.Level != "" {
		must = append(must, map[string]any{"term": map[string]any{"level": q.Level}})
	}
	if q.Query != "" {
		must = append(must, map[string]any{"match": map[string]any{"message": q.Query}})
	}

	page := q.Page
	if page < 1 {
		page = 1
	}
	pageSize := q.PageSize
	if pageSize < 1 || pageSize > 1000 {
		pageSize = 100
	}

	dsl := map[string]any{
		"from": (page - 1) * pageSize,
		"size": pageSize,
		"query": map[string]any{
			"bool": map[string]any{"must": must},
		},
		"sort": []map[string]any{
			{"event_timestamp": map[string]any{"order": "desc"}},
			{"ingest_timestamp": map[string]any{"order": "desc"}},
		},
	}
	body, err := json.Marshal(dsl)
	if err != nil {
		return SearchResult{}, err
	}

	indices := indicesForRange(q.TenantID, q.From, q.To)
	req := opensearchapi.SearchRequest{
		Index: indices,
		Body:  bytes.NewReader(body),
	}
	resp, err := req.Do(ctx, c.os)
	if err != nil {
		return SearchResult{}, fmt.Errorf("opensearch search: %w", err)
	}
	defer resp.Body.Close()
	if resp.IsError() {
		return SearchResult{}, fmt.Errorf("opensearch search error: %s", resp.Status())
	}

	var raw struct {
		Hits struct {
			Total struct {
				Value int `json:"value"`
			} `json:"total"`
			Hits []struct {
				Source models.LogEntry `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return SearchResult{}, fmt.Errorf("opensearch decode: %w", err)
	}
	entries := make([]models.LogEntry, len(raw.Hits.Hits))
	for i, h := range raw.Hits.Hits {
		entries[i] = h.Source
	}
	return SearchResult{Total: raw.Hits.Total.Value, Entries: entries}, nil
}

// indicesForRange enumerates daily index names covering [from, to].
func indicesForRange(tenantID string, from, to time.Time) []string {
	var indices []string
	seen := make(map[string]struct{})
	for t := from.UTC().Truncate(24 * time.Hour); !t.After(to.UTC()); t = t.AddDate(0, 0, 1) {
		name := IndexName(tenantID, t)
		if _, ok := seen[name]; !ok {
			indices = append(indices, name)
			seen[name] = struct{}{}
		}
	}
	return indices
}

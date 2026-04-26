package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	osclient "github.com/hossainshakhawat/distributed-logging/store-opensearch/client"
	"github.com/hossainshakhawat/distributed-logging/store-kafka/models"
)

// ── mock searcher ─────────────────────────────────────────────────────────────

type mockSearcher struct {
	result osclient.SearchResult
	err    error
}

func (m *mockSearcher) Search(_ context.Context, _ osclient.SearchQuery) (osclient.SearchResult, error) {
	return m.result, m.err
}

// ── helpers ───────────────────────────────────────────────────────────────────

func newServer(keys map[string]string, searcher Searcher) *Server {
	return NewServer(Config{
		ListenAddr:       ":0",
		HotRetentionDays: 7,
		ValidAPIKeys:     keys,
	}, searcher)
}

// coldServer routes all queries to cold storage (hot retention = 0 days).
func coldServer(keys map[string]string) *Server {
	return NewServer(Config{
		ListenAddr:       ":0",
		HotRetentionDays: 0,
		ValidAPIKeys:     keys,
	}, nil)
}

func searchRequest(tenantID, apiKey string, params map[string]string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/logs/search", nil)
	if tenantID != "" {
		req.Header.Set("X-Tenant-ID", tenantID)
	}
	if apiKey != "" {
		req.Header.Set("X-API-Key", apiKey)
	}
	q := req.URL.Query()
	for k, v := range params {
		q.Set(k, v)
	}
	req.URL.RawQuery = q.Encode()
	return req
}

// ── /healthz ─────────────────────────────────────────────────────────────────

func TestHealthz(t *testing.T) {
	srv := newServer(nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("healthz: got %d, want 200", rr.Code)
	}
}

// ── handleSearch method check ─────────────────────────────────────────────────

func TestSearch_wrongMethod(t *testing.T) {
	srv := newServer(nil, nil)
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		req := httptest.NewRequest(method, "/logs/search", nil)
		rr := httptest.NewRecorder()
		srv.ServeHTTP(rr, req)
		if rr.Code != http.StatusMethodNotAllowed {
			t.Errorf("method %s: got %d, want 405", method, rr.Code)
		}
	}
}

// ── authenticate ─────────────────────────────────────────────────────────────

func TestAuthenticate_devMode(t *testing.T) {
	s := &Server{cfg: Config{ValidAPIKeys: nil}}
	if !s.authenticate("any-tenant", "any-key") {
		t.Fatal("dev mode should always authenticate")
	}
	if !s.authenticate("any-tenant", "") {
		t.Fatal("dev mode should work with empty api key")
	}
}

func TestAuthenticate_emptyTenantID(t *testing.T) {
	s := &Server{cfg: Config{ValidAPIKeys: nil}}
	if s.authenticate("", "key") {
		t.Fatal("empty tenantID should always fail")
	}
}

func TestAuthenticate_validKey(t *testing.T) {
	s := &Server{cfg: Config{ValidAPIKeys: map[string]string{"t1": "secret"}}}
	if !s.authenticate("t1", "secret") {
		t.Fatal("valid key should succeed")
	}
}

func TestAuthenticate_wrongKey(t *testing.T) {
	s := &Server{cfg: Config{ValidAPIKeys: map[string]string{"t1": "secret"}}}
	if s.authenticate("t1", "wrong") {
		t.Fatal("wrong key should fail")
	}
}

func TestAuthenticate_unknownTenant(t *testing.T) {
	s := &Server{cfg: Config{ValidAPIKeys: map[string]string{"t1": "secret"}}}
	if s.authenticate("t2", "secret") {
		t.Fatal("unknown tenant should fail")
	}
}

// ── handleSearch authentication ───────────────────────────────────────────────

func TestSearch_unauthorized_missingTenant(t *testing.T) {
	srv := coldServer(nil) // dev mode but no tenant
	req := httptest.NewRequest(http.MethodGet, "/logs/search", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("missing tenant: got %d, want 401", rr.Code)
	}
}

func TestSearch_unauthorized_wrongKey(t *testing.T) {
	srv := newServer(map[string]string{"t1": "correct"}, nil)
	req := searchRequest("t1", "wrong", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("wrong key: got %d, want 401", rr.Code)
	}
}

// ── parseRequest ─────────────────────────────────────────────────────────────

func TestParseRequest_defaults(t *testing.T) {
	s := &Server{cfg: Config{}}
	req := httptest.NewRequest(http.MethodGet, "/logs/search", nil)
	sr, err := s.parseRequest(req, "t1")
	if err != nil {
		t.Fatalf("parseRequest: %v", err)
	}
	if sr.TenantID != "t1" {
		t.Errorf("TenantID: got %q, want %q", sr.TenantID, "t1")
	}
	if sr.Page != 1 {
		t.Errorf("Page default: got %d, want 1", sr.Page)
	}
	if sr.PageSize != 100 {
		t.Errorf("PageSize default: got %d, want 100", sr.PageSize)
	}
	if sr.To.IsZero() {
		t.Error("To should default to now")
	}
	if sr.From.IsZero() {
		t.Error("From should default to 1hr ago")
	}
	diff := sr.To.Sub(sr.From)
	if diff < 59*time.Minute || diff > 61*time.Minute {
		t.Errorf("From should be ~1hr before To, got diff=%v", diff)
	}
}

func TestParseRequest_customParams(t *testing.T) {
	s := &Server{cfg: Config{}}
	now := time.Now().UTC()
	from := now.Add(-24 * time.Hour)
	req := httptest.NewRequest(http.MethodGet, "/logs/search", nil)
	q := req.URL.Query()
	q.Set("service", "svc-a")
	q.Set("level", "ERROR")
	q.Set("q", "crash")
	q.Set("from", from.Format(time.RFC3339))
	q.Set("to", now.Format(time.RFC3339))
	q.Set("page", "2")
	q.Set("page_size", "50")
	req.URL.RawQuery = q.Encode()

	sr, err := s.parseRequest(req, "t1")
	if err != nil {
		t.Fatalf("parseRequest: %v", err)
	}
	if sr.Service != "svc-a" {
		t.Errorf("Service: got %q", sr.Service)
	}
	if sr.Level != "ERROR" {
		t.Errorf("Level: got %q", sr.Level)
	}
	if sr.Query != "crash" {
		t.Errorf("Query: got %q", sr.Query)
	}
	if sr.Page != 2 {
		t.Errorf("Page: got %d, want 2", sr.Page)
	}
	if sr.PageSize != 50 {
		t.Errorf("PageSize: got %d, want 50", sr.PageSize)
	}
}

func TestParseRequest_pageSize_capped(t *testing.T) {
	s := &Server{cfg: Config{}}
	req := httptest.NewRequest(http.MethodGet, "/logs/search", nil)
	q := req.URL.Query()
	q.Set("page_size", "9999") // above 1000 → ignored, stays 100
	req.URL.RawQuery = q.Encode()
	sr, _ := s.parseRequest(req, "t1")
	if sr.PageSize != 100 {
		t.Errorf("page_size>1000 should fall back to 100, got %d", sr.PageSize)
	}
}

func TestParseRequest_invalidPage_ignored(t *testing.T) {
	s := &Server{cfg: Config{}}
	req := httptest.NewRequest(http.MethodGet, "/logs/search", nil)
	q := req.URL.Query()
	q.Set("page", "abc")
	req.URL.RawQuery = q.Encode()
	sr, _ := s.parseRequest(req, "t1")
	if sr.Page != 1 {
		t.Errorf("invalid page should default to 1, got %d", sr.Page)
	}
}

// ── queryCold ─────────────────────────────────────────────────────────────────

func TestQueryCold_returnsEmpty(t *testing.T) {
	s := &Server{cfg: Config{HotRetentionDays: 7}}
	req := SearchRequest{TenantID: "t1", Page: 1, PageSize: 100}
	resp := s.queryCold(req)
	if resp.Total != 0 {
		t.Errorf("cold total: got %d, want 0", resp.Total)
	}
	if len(resp.Entries) != 0 {
		t.Errorf("cold entries: got %d, want 0", len(resp.Entries))
	}
	if resp.Page != 1 || resp.PageSize != 100 {
		t.Errorf("cold pagination: page=%d size=%d", resp.Page, resp.PageSize)
	}
}

// ── route ─────────────────────────────────────────────────────────────────────

func TestRoute_coldPath_oldData(t *testing.T) {
	srv := coldServer(nil) // HotRetentionDays=0 → everything routes to cold
	srv.cfg.HotRetentionDays = 7
	req := httptest.NewRequest(http.MethodGet, "/logs/search", nil)
	// Use a From that is older than HotRetentionDays (8 days ago).
	sr := SearchRequest{
		TenantID: "t1",
		From:     time.Now().UTC().AddDate(0, 0, -8),
		To:       time.Now().UTC().AddDate(0, 0, -7),
		Page:     1,
		PageSize: 10,
	}
	resp := srv.route(req, sr)
	if resp.Total != 0 {
		t.Errorf("old data should route to cold, expected 0 total, got %d", resp.Total)
	}
}

func TestRoute_hotPath_recentData(t *testing.T) {
	result := osclient.SearchResult{
		Total:   2,
		Entries: []models.LogEntry{{LogID: "l1"}, {LogID: "l2"}},
	}
	searcher := &mockSearcher{result: result}
	srv := newServer(nil, searcher)

	req := httptest.NewRequest(http.MethodGet, "/logs/search", nil)
	sr := SearchRequest{
		TenantID: "t1",
		From:     time.Now().UTC().Add(-30 * time.Minute), // recent
		To:       time.Now().UTC(),
		Page:     1,
		PageSize: 100,
	}
	resp := srv.route(req, sr)
	if resp.Total != 2 {
		t.Errorf("recent data should route to hot, got total=%d", resp.Total)
	}
	if len(resp.Entries) != 2 {
		t.Errorf("expected 2 entries from hot, got %d", len(resp.Entries))
	}
}

// ── handleSearch integration ──────────────────────────────────────────────────

func TestSearch_coldPath_returnsJSON(t *testing.T) {
	srv := coldServer(nil) // HotRetentionDays=0, dev mode
	// Pass from far in the past to ensure cold routing.
	req := searchRequest("t1", "", map[string]string{
		"from": time.Now().UTC().Add(-100 * 24 * time.Hour).Format(time.RFC3339),
	})
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("cold search: got %d, want 200 (body: %s)", rr.Code, rr.Body.String())
	}
	var resp SearchResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Entries == nil {
		t.Error("entries should be non-nil (empty slice)")
	}
}

func TestSearch_hotPath_returnsSearcherResult(t *testing.T) {
	searcher := &mockSearcher{result: osclient.SearchResult{
		Total:   1,
		Entries: []models.LogEntry{{LogID: "abc", Message: "test"}},
	}}
	srv := newServer(nil, searcher)

	req := searchRequest("t1", "", nil) // defaults: recent From → hot path
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("hot search: got %d, want 200", rr.Code)
	}
	var resp SearchResponse
	_ = json.NewDecoder(rr.Body).Decode(&resp)
	if resp.Total != 1 {
		t.Errorf("expected total=1, got %d", resp.Total)
	}
}

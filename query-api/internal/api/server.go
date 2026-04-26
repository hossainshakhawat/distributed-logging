package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/distributed-logging/shared/models"
)

// Config holds query API settings.
type Config struct {
	ListenAddr       string
	OpenSearchAddr   string
	HotRetentionDays int
	ValidAPIKeys     map[string]string // tenantID -> apiKey
}

// Server is the HTTP query server.
type Server struct {
	cfg Config
	mux *http.ServeMux
}

// NewServer constructs and wires the server.
func NewServer(cfg Config) *Server {
	s := &Server{cfg: cfg, mux: http.NewServeMux()}
	s.mux.HandleFunc("/logs/search", s.handleSearch)
	s.mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// SearchRequest encapsulates query parameters.
type SearchRequest struct {
	TenantID string
	Service  string
	Level    string
	Query    string
	From     time.Time
	To       time.Time
	Page     int
	PageSize int
}

// SearchResponse wraps result entries and pagination info.
type SearchResponse struct {
	Total    int               `json:"total"`
	Page     int               `json:"page"`
	PageSize int               `json:"page_size"`
	Entries  []models.LogEntry `json:"entries"`
}

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	tenantID, apiKey := r.Header.Get("X-Tenant-ID"), r.Header.Get("X-API-Key")
	if !s.authenticate(tenantID, apiKey) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	req, err := s.parseRequest(r, tenantID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	results := s.route(req)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

func (s *Server) parseRequest(r *http.Request, tenantID string) (SearchRequest, error) {
	q := r.URL.Query()
	from, _ := time.Parse(time.RFC3339, q.Get("from"))
	to, _ := time.Parse(time.RFC3339, q.Get("to"))
	if to.IsZero() {
		to = time.Now().UTC()
	}
	if from.IsZero() {
		from = to.Add(-1 * time.Hour)
	}
	return SearchRequest{
		TenantID: tenantID,
		Service:  q.Get("service"),
		Level:    q.Get("level"),
		Query:    q.Get("q"),
		From:     from,
		To:       to,
		Page:     1,
		PageSize: 100,
	}, nil
}

// route decides whether to hit hot (OpenSearch) or cold (S3/Athena) storage.
func (s *Server) route(req SearchRequest) SearchResponse {
	hotCutoff := time.Now().UTC().AddDate(0, 0, -s.cfg.HotRetentionDays)
	if req.From.After(hotCutoff) {
		return s.queryHot(req)
	}
	return s.queryCold(req)
}

// queryHot queries OpenSearch (stubbed).
func (s *Server) queryHot(req SearchRequest) SearchResponse {
	// In production: build an OpenSearch DSL query and execute it.
	return SearchResponse{
		Total:    0,
		Page:     req.Page,
		PageSize: req.PageSize,
		Entries:  []models.LogEntry{},
	}
}

// queryCold queries S3/Athena for archived logs (stubbed).
func (s *Server) queryCold(req SearchRequest) SearchResponse {
	// In production: submit an Athena/Trino query and stream results.
	return SearchResponse{
		Total:    0,
		Page:     req.Page,
		PageSize: req.PageSize,
		Entries:  []models.LogEntry{},
	}
}

func (s *Server) authenticate(tenantID, apiKey string) bool {
	if tenantID == "" {
		return false
	}
	if len(s.cfg.ValidAPIKeys) == 0 {
		return true
	}
	expected, ok := s.cfg.ValidAPIKeys[tenantID]
	return ok && expected == apiKey
}

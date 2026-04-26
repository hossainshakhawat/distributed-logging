package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/distributed-logging/shared/models"
	osclient "github.com/distributed-logging/store-opensearch/client"
)

// Config holds query API settings.
type Config struct {
	ListenAddr       string
	HotRetentionDays int
	ValidAPIKeys     map[string]string // tenantID -> apiKey
}

// Server is the HTTP query server.
type Server struct {
	cfg      Config
	osClient *osclient.Client
	mux      *http.ServeMux
}

// NewServer constructs and wires the server.
func NewServer(cfg Config, osClient *osclient.Client) *Server {
	s := &Server{cfg: cfg, osClient: osClient, mux: http.NewServeMux()}
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

	results := s.route(r, req)
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
	page := 1
	if p := q.Get("page"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n > 0 {
			page = n
		}
	}
	pageSize := 100
	if ps := q.Get("page_size"); ps != "" {
		if n, err := strconv.Atoi(ps); err == nil && n > 0 && n <= 1000 {
			pageSize = n
		}
	}
	return SearchRequest{
		TenantID: tenantID,
		Service:  q.Get("service"),
		Level:    q.Get("level"),
		Query:    q.Get("q"),
		From:     from,
		To:       to,
		Page:     page,
		PageSize: pageSize,
	}, nil
}

// route decides whether to hit hot (OpenSearch) or cold (S3/Athena) storage.
func (s *Server) route(r *http.Request, req SearchRequest) SearchResponse {
	hotCutoff := time.Now().UTC().AddDate(0, 0, -s.cfg.HotRetentionDays)
	if req.From.After(hotCutoff) {
		return s.queryHot(r, req)
	}
	return s.queryCold(req)
}

// queryHot queries OpenSearch for recent logs.
func (s *Server) queryHot(r *http.Request, req SearchRequest) SearchResponse {
	result, err := s.osClient.Search(r.Context(), osclient.SearchQuery{
		TenantID: req.TenantID,
		Service:  req.Service,
		Level:    req.Level,
		Query:    req.Query,
		From:     req.From,
		To:       req.To,
		Page:     req.Page,
		PageSize: req.PageSize,
	})
	if err != nil {
		log.Printf("query-api: opensearch search: %v", err)
		return SearchResponse{Page: req.Page, PageSize: req.PageSize, Entries: []models.LogEntry{}}
	}
	return SearchResponse{
		Total:    result.Total,
		Page:     req.Page,
		PageSize: req.PageSize,
		Entries:  result.Entries,
	}
}

// queryCold queries S3/Athena for archived logs (stubbed — wire Athena/Trino here).
func (s *Server) queryCold(req SearchRequest) SearchResponse {
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

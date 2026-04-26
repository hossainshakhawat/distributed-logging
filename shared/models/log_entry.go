package models

import "time"

// LogEntry is the canonical log record used across all services.
type LogEntry struct {
	LogID           string            `json:"log_id"`
	TenantID        string            `json:"tenant_id"`
	Service         string            `json:"service"`
	Host            string            `json:"host"`
	Container       string            `json:"container,omitempty"`
	Environment     string            `json:"environment"`
	Level           string            `json:"level"`
	Message         string            `json:"message"`
	TraceID         string            `json:"trace_id,omitempty"`
	RequestID       string            `json:"request_id,omitempty"`
	EventTimestamp  time.Time         `json:"event_timestamp"`
	IngestTimestamp time.Time         `json:"ingest_timestamp"`
	Fields          map[string]string `json:"fields,omitempty"`
}

// LogBatch is what agents send to the ingestion gateway.
type LogBatch struct {
	AgentID string     `json:"agent_id"`
	Entries []LogEntry `json:"entries"`
	SentAt  time.Time  `json:"sent_at"`
}

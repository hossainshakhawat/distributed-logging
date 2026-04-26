package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/hossainshakhawat/distributed-logging/store-kafka/kafka"
	"github.com/hossainshakhawat/distributed-logging/store-kafka/models"
)

// Rule defines an alerting condition.
type Rule struct {
	Name            string
	TenantID        string // "*" matches any tenant
	Level           string // filter by level (empty = any)
	MessageContains string // substring match on message (empty = any)
	Threshold       int    // number of matching events in window to trigger
	WindowSecs      int    // tumbling window size in seconds
	Webhook         string // URL to POST alert notification
}

// windowState tracks event count within a tumbling window.
type windowState struct {
	count     int
	windowEnd time.Time
}

// Engine consumes normalised log entries and evaluates alert rules.
type Engine struct {
	consumer        kafka.Consumer
	rules           []Rule
	normalizedTopic string
	state           map[string]*windowState // key: ruleName+tenantID
	mu              sync.Mutex
	done            chan struct{}
	client          *http.Client
}

// NewEngine creates an Engine.
func NewEngine(consumer kafka.Consumer, rules []Rule, normalizedTopic string) *Engine {
	return &Engine{
		consumer:        consumer,
		rules:           rules,
		normalizedTopic: normalizedTopic,
		state:           make(map[string]*windowState),
		done:            make(chan struct{}),
		client:          &http.Client{Timeout: 5 * time.Second},
	}
}

// Start subscribes to Kafka and begins evaluation.
func (e *Engine) Start() error {
	if err := e.consumer.Subscribe([]string{e.normalizedTopic}); err != nil {
		return err
	}
	go e.loop()
	return nil
}

// Stop signals the engine to stop.
func (e *Engine) Stop() { close(e.done) }

func (e *Engine) loop() {
	ctx := context.Background()
	for {
		select {
		case <-e.done:
			return
		default:
		}
		msg, err := e.consumer.Poll(ctx)
		if err != nil {
			log.Printf("alert: poll: %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		var entry models.LogEntry
		if err := kafka.UnmarshalValue(msg, &entry); err != nil {
			continue
		}
		e.evaluate(entry)
	}
}

func (e *Engine) evaluate(entry models.LogEntry) {
	now := time.Now().UTC()
	for _, rule := range e.rules {
		if !e.matches(rule, entry) {
			continue
		}
		key := rule.Name + "|" + entry.TenantID
		e.mu.Lock()
		ws, ok := e.state[key]
		if !ok || now.After(ws.windowEnd) {
			ws = &windowState{
				count:     0,
				windowEnd: now.Add(time.Duration(rule.WindowSecs) * time.Second),
			}
			e.state[key] = ws
		}
		ws.count++
		count := ws.count
		e.mu.Unlock()

		if count >= rule.Threshold {
			go e.fire(rule, entry, count)
		}
	}
}

func (e *Engine) matches(rule Rule, entry models.LogEntry) bool {
	if rule.TenantID != "*" && rule.TenantID != entry.TenantID {
		return false
	}
	if rule.Level != "" && !strings.EqualFold(rule.Level, entry.Level) {
		return false
	}
	if rule.MessageContains != "" &&
		!strings.Contains(strings.ToLower(entry.Message), strings.ToLower(rule.MessageContains)) {
		return false
	}
	return true
}

type alertPayload struct {
	Rule      string    `json:"rule"`
	TenantID  string    `json:"tenant_id"`
	Service   string    `json:"service"`
	Count     int       `json:"count"`
	FiredAt   time.Time `json:"fired_at"`
	SampleMsg string    `json:"sample_message"`
}

func (e *Engine) fire(rule Rule, entry models.LogEntry, count int) {
	payload := alertPayload{
		Rule:      rule.Name,
		TenantID:  entry.TenantID,
		Service:   entry.Service,
		Count:     count,
		FiredAt:   time.Now().UTC(),
		SampleMsg: entry.Message,
	}
	b, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, rule.Webhook, bytes.NewReader(b))
	if err != nil {
		log.Printf("alert: build request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.client.Do(req)
	if err != nil {
		log.Printf("alert: webhook %s: %v", rule.Webhook, err)
		return
	}
	resp.Body.Close()
	log.Printf("alert fired: rule=%s tenant=%s count=%d", rule.Name, entry.TenantID, count)
}

# Distributed Logging

A production-grade distributed log aggregation system implemented in Go, organised as a Go workspace (multi-module monorepo).

## Architecture

```
Applications
  ↓
Log Agent (log-agent)          — batch, compress, retry, buffer locally
  ↓
Ingestion Gateway              — auth, rate-limit, decompress → Kafka
  ↓
Kafka  (logs-raw)
  ↓
Stream Processor               — parse, normalise, enrich, DLQ → Kafka (logs-normalized)
  ├── OpenSearch               — hot searchable storage (7–30 days)
  ├── S3 / GCS                 — cold archive (Parquet / gzip)
  └── Alert Engine             — windowed rule evaluation → webhook
  
User → Query API               — routes to hot or cold storage
User → Tail Service            — SSE live stream from Kafka
```

## Repository Layout

```
distributed-logging/
├── go.work                    # Go workspace (ties all modules together)
├── docker-compose.yml         # Full local stack
├── shared/                    # Shared models, Kafka interfaces, config helpers
│   ├── models/log_entry.go
│   ├── kafka/client.go
│   └── config/env.go
├── log-agent/                 # Reads log files, ships batches to gateway
├── ingestion-gateway/         # HTTP entry point — auth, rate-limit, → Kafka
├── stream-processor/          # Kafka consumer — normalise, enrich, route
├── query-api/                 # REST search API over OpenSearch / cold storage
├── tail-service/              # Live log tailing via Server-Sent Events
└── alert-engine/              # Windowed rule evaluation and webhook alerts
```

## Services

| Service | Port | Responsibility |
|---|---|---|
| ingestion-gateway | 8080 | Receive log batches from agents |
| query-api | 8081 | Search logs (hot + cold) |
| tail-service | 8082 | Live SSE log streaming |
| stream-processor | — | Normalise and route logs |
| alert-engine | — | Rule evaluation and notifications |
| log-agent | — | Tail files and ship to gateway |

## Quick Start

### Prerequisites

- Go 1.22+
- Docker + Docker Compose

### Run the full stack

```bash
git clone <repo>
cd distributed-logging
mkdir -p demo-logs
docker compose up --build
```

### Build all modules locally

```bash
cd distributed-logging
go work sync
go build ./...
```

### Run individual services

```bash
# Ingestion Gateway
cd ingestion-gateway
LISTEN_ADDR=:8080 go run ./cmd/gateway

# Query API
cd query-api
LISTEN_ADDR=:8081 OPENSEARCH_ADDR=http://localhost:9200 go run ./cmd/api

# Tail Service
cd tail-service
LISTEN_ADDR=:8082 go run ./cmd/tail

# Stream Processor
cd stream-processor
go run ./cmd/processor

# Alert Engine
cd alert-engine
go run ./cmd/alert

# Log Agent
cd log-agent
GATEWAY_URL=http://localhost:8080 TENANT_ID=tenant-a SERVICE_NAME=my-service LOG_PATH=/tmp/app.log go run ./cmd/agent
```

## API Reference

### Ingestion Gateway

#### `POST /ingest`

Ships a batch of logs. Used by the log-agent.

```
Headers:
  Content-Type: application/json
  Content-Encoding: gzip          (required when body is gzip-compressed)
  X-Tenant-ID: <tenant>
  X-API-Key: <key>
```

Body: `LogBatch` JSON (see `shared/models/log_entry.go`)

Response: `202 Accepted`

---

### Query API

#### `GET /logs/search`

```
Headers:
  X-Tenant-ID: <tenant>
  X-API-Key: <key>

Query params:
  service=<name>
  level=ERROR|WARN|INFO|DEBUG
  q=<full-text query>
  from=<RFC3339>
  to=<RFC3339>
```

Response:

```json
{
  "total": 42,
  "page": 1,
  "page_size": 100,
  "entries": [ ... ]
}
```

---

### Tail Service

#### `GET /tail`  (SSE)

```
Headers:
  X-Tenant-ID: <tenant>

Query params:
  service=<name>   (optional filter)
  level=ERROR      (optional filter)
```

Response: `text/event-stream`

Each event: `data: <LogEntry JSON>\n\n`

Example with curl:
```bash
curl -N -H "X-Tenant-ID: tenant-a" \
  "http://localhost:8082/tail?service=payment&level=ERROR"
```

---

## Kafka Topics

| Topic | Producer | Consumers |
|---|---|---|
| `logs-raw` | ingestion-gateway | stream-processor |
| `logs-normalized` | stream-processor | alert-engine, tail-service, indexer |
| `logs-dead-letter` | stream-processor | ops/monitoring |

Partition key: `tenantID|serviceName`

---

## Configuration (Environment Variables)

All services are configured entirely through environment variables.

### log-agent

| Variable | Default | Description |
|---|---|---|
| `GATEWAY_URL` | `http://localhost:8080` | Ingestion gateway base URL |
| `TENANT_ID` | `default` | Tenant identifier |
| `SERVICE_NAME` | `unknown` | Logical service name |
| `ENVIRONMENT` | `production` | Deployment environment |
| `LOG_PATH` | `/var/log/app/app.log` | Log file to tail |
| `BUFFER_DIR` | `/tmp/log-agent-buffer` | Local disk buffer for offline mode |

### ingestion-gateway

| Variable | Default | Description |
|---|---|---|
| `LISTEN_ADDR` | `:8080` | HTTP listen address |

### query-api

| Variable | Default | Description |
|---|---|---|
| `LISTEN_ADDR` | `:8081` | HTTP listen address |
| `OPENSEARCH_ADDR` | `http://localhost:9200` | OpenSearch endpoint |

### tail-service

| Variable | Default | Description |
|---|---|---|
| `LISTEN_ADDR` | `:8082` | HTTP listen address |

---

## Design Decisions

### Deduplication
Each log-agent attaches a `log_id` derived from `SHA-256(agent_id + line + timestamp)`. The stream-processor can use this as the OpenSearch document ID for idempotent upserts.

### Ordering
Global ordering is not guaranteed. Ordering is guaranteed per Kafka partition (tenant+service). Results are sorted by `event_timestamp`, with `ingest_timestamp` as tie-breaker.

### Retention tiers
| Tier | Store | Retention |
|---|---|---|
| Hot | OpenSearch | 7–30 days (per plan) |
| Cold | S3/GCS Parquet/gzip | Unlimited |

### Failure modes
| Failure | Behaviour |
|---|---|
| Gateway down | Agent buffers to disk, retries on reconnect |
| Kafka unavailable | Gateway returns 5xx, agents back-pressure |
| Stream-processor crash | Resumes from committed Kafka offsets |
| OpenSearch slow | Kafka consumer lag increases; autoscale processors |
| Bad log format | Routed to `logs-dead-letter` topic |

---

## Swapping Stubs for Real Implementations

The `shared/kafka` package defines `Producer` and `Consumer` interfaces. The services currently use `StubProducer` / `StubConsumer` (stdout logging) so they compile and run without a Kafka cluster. Replace these in each service's `main.go` with a real driver such as:

- [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go)
- [segmentio/kafka-go](https://github.com/segmentio/kafka-go)
- [twmb/franz-go](https://github.com/twmb/franz-go)

Similarly, `query-api` stubs OpenSearch calls. Replace `queryHot` / `queryCold` with real [opensearch-go](https://github.com/opensearch-project/opensearch-go) calls.

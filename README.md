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
├── store/
│   ├── kafka/                 # Canonical Kafka interfaces, models & franz-go driver
│   │   ├── kafka/             # Producer/Consumer interfaces + Message type (shared)
│   │   ├── models/            # LogEntry, LogBatch types (shared)
│   │   ├── consumer/          # franz-go Consumer implementation
│   │   └── producer/          # franz-go Producer implementation
│   ├── opensearch/            # OpenSearch client
│   ├── redis/                 # Redis client
│   └── s3/                    # S3 client
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

- Go 1.25+
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

Body: `LogBatch` JSON (see `store/kafka/models/log_entry.go`)

Response: `202 Accepted`

---

### Query API

#### `GET /logs/search`

```
Headers:
  X-Tenant-ID: <tenant>
  X-API-Key: <key>           (omit when api_keys is empty — dev mode)

Query params:
  service=<name>             (optional)
  level=ERROR|WARN|INFO|DEBUG (optional)
  q=<full-text query>        (optional)
  from=<RFC3339>             (default: 1 hour ago)
  to=<RFC3339>               (default: now)
  page=<int>                 (default: 1)
  page_size=<int>            (default: 100, max: 1000)
```

Requests with `from` within the last `hot_retention_days` (default 7) are served from **OpenSearch**. Older time ranges hit the cold path (S3/Athena — stub, returns empty until wired).

**Response:**

```json
{
  "total": 142,
  "page": 1,
  "page_size": 100,
  "entries": [
    {
      "log_id": "a1b2c3d4",
      "tenant_id": "tenant-a",
      "service": "payment",
      "host": "pod-1",
      "environment": "production",
      "level": "ERROR",
      "message": "connection timeout to db",
      "event_timestamp": "2026-04-26T10:05:00Z",
      "ingest_timestamp": "2026-04-26T10:05:01Z"
    }
  ]
}
```

**Examples:**

```bash
# All errors in the last hour
curl -s \
  -H "X-Tenant-ID: tenant-a" \
  -H "X-API-Key: secret-a" \
  "http://localhost:8081/logs/search?level=ERROR"

# Filter by service
curl -s \
  -H "X-Tenant-ID: tenant-a" \
  -H "X-API-Key: secret-a" \
  "http://localhost:8081/logs/search?service=payment"

# Full-text search
curl -s \
  -H "X-Tenant-ID: tenant-a" \
  -H "X-API-Key: secret-a" \
  "http://localhost:8081/logs/search?q=connection+refused"

# Combine filters with a time range
curl -s \
  -H "X-Tenant-ID: tenant-a" \
  -H "X-API-Key: secret-a" \
  "http://localhost:8081/logs/search?service=payment&level=ERROR&q=timeout&from=2026-04-26T00:00:00Z&to=2026-04-26T12:00:00Z"

# Pagination — page 2, 50 results per page
curl -s \
  -H "X-Tenant-ID: tenant-a" \
  -H "X-API-Key: secret-a" \
  "http://localhost:8081/logs/search?level=WARN&page=2&page_size=50"
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

## Configuration

Each service reads a `config.yml` file at startup and supports environment variable overrides for every key. Nested YAML keys map to environment variables by replacing `.` and `-` with `_` and uppercasing (e.g. `kafka.brokers` → `KAFKA_BROKERS`).

A custom config file path can be forced with the `CONFIG_FILE` environment variable.

---

### log-agent

Config file: `log-agent/config.yml`

```yaml
gateway_url: http://localhost:8080
tenant_id: default
service_name: unknown
environment: production
log_paths:
  - /var/log/app/app.log
batch_size: 100
flush_interval_secs: 5
buffer_dir: /tmp/log-agent-buffer
```

| Key / Env var | Default | Description |
|---|---|---|
| `gateway_url` / `GATEWAY_URL` | `http://localhost:8080` | Ingestion gateway base URL |
| `tenant_id` / `TENANT_ID` | `default` | Tenant identifier |
| `service_name` / `SERVICE_NAME` | `unknown` | Logical service name |
| `environment` / `ENVIRONMENT` | `production` | Deployment environment |
| `log_paths` / `LOG_PATH` | `[/var/log/app/app.log]` | Log files to tail |
| `batch_size` / `BATCH_SIZE` | `100` | Entries per batch before forced flush |
| `flush_interval_secs` / `FLUSH_INTERVAL_SECS` | `5` | Periodic flush interval (seconds) |
| `buffer_dir` / `BUFFER_DIR` | `/tmp/log-agent-buffer` | Local disk buffer for offline mode |

---

### ingestion-gateway

Config file: `ingestion-gateway/config.yml`

```yaml
listen_addr: ":8080"
rate_limit_rps: 1000

kafka:
  brokers:
    - localhost:9092
  topics:
    logs_raw: logs-raw

# Tenant → API key pairs (override at deploy time)
api_keys:
  tenant-a: secret-a
  tenant-b: secret-b
```

| Key / Env var | Default | Description |
|---|---|---|
| `listen_addr` / `LISTEN_ADDR` | `:8080` | HTTP listen address |
| `rate_limit_rps` / `RATE_LIMIT_RPS` | `1000` | Per-tenant requests per second |
| `kafka.brokers` / `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker list |
| `kafka.topics.logs_raw` / `KAFKA_TOPICS_LOGS_RAW` | `logs-raw` | Topic for raw log batches |
| `api_keys.<tenant>` | — | Map of tenantID → API key |

---

### stream-processor

Config file: `stream-processor/config.yml`

```yaml
kafka:
  brokers:
    - localhost:9092
  consumer_group: stream-processor
  topics:
    logs_raw: logs-raw
    logs_normalized: logs-normalized
    dead_letter: logs-dead-letter

opensearch:
  addresses:
    - http://localhost:9200
  username: admin
  password: Admin@12345

s3:
  bucket: distributed-logs
  region: us-east-1
  endpoint: ""          # set to MinIO/LocalStack URL for local dev

redis:
  addr: localhost:6379
  password: ""
  db: 0

indexer:
  batch_size: 500
  flush_interval_seconds: 5

archiver:
  batch_size: 1000
  flush_interval_seconds: 30

dedup_ttl_seconds: 300
```

| Key / Env var | Default | Description |
|---|---|---|
| `kafka.brokers` / `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker list |
| `kafka.consumer_group` / `KAFKA_CONSUMER_GROUP` | `stream-processor` | Consumer group ID |
| `kafka.topics.logs_raw` / `KAFKA_TOPICS_LOGS_RAW` | `logs-raw` | Topic to consume raw batches from |
| `kafka.topics.logs_normalized` / `KAFKA_TOPICS_LOGS_NORMALIZED` | `logs-normalized` | Topic to publish normalised entries to |
| `kafka.topics.dead_letter` / `KAFKA_TOPICS_DEAD_LETTER` | `logs-dead-letter` | Dead-letter topic for unparseable messages |
| `opensearch.addresses` / `OPENSEARCH_ADDRESSES` | `http://localhost:9200` | OpenSearch endpoint(s) |
| `opensearch.username` / `OPENSEARCH_USERNAME` | `admin` | OpenSearch username |
| `opensearch.password` / `OPENSEARCH_PASSWORD` | `Admin@12345` | OpenSearch password |
| `s3.bucket` / `S3_BUCKET` | `distributed-logs` | S3 bucket for cold archive |
| `s3.region` / `S3_REGION` | `us-east-1` | AWS region |
| `s3.endpoint` / `S3_ENDPOINT` | `""` | Custom endpoint (MinIO / LocalStack) |
| `redis.addr` / `REDIS_ADDR` | `localhost:6379` | Redis address for dedup cache |
| `redis.password` / `REDIS_PASSWORD` | `""` | Redis password |
| `redis.db` / `REDIS_DB` | `0` | Redis DB index |
| `indexer.batch_size` / `INDEXER_BATCH_SIZE` | `500` | Entries per OpenSearch bulk request |
| `indexer.flush_interval_seconds` / `INDEXER_FLUSH_INTERVAL_SECONDS` | `5` | Indexer flush interval |
| `archiver.batch_size` / `ARCHIVER_BATCH_SIZE` | `1000` | Entries per S3 upload |
| `archiver.flush_interval_seconds` / `ARCHIVER_FLUSH_INTERVAL_SECONDS` | `30` | Archiver flush interval |
| `dedup_ttl_seconds` / `DEDUP_TTL_SECONDS` | `300` | Redis TTL for dedup keys (seconds) |

---

### query-api

Config file: `query-api/config.yml`

```yaml
listen_addr: ":8081"
hot_retention_days: 7

opensearch:
  addresses:
    - http://localhost:9200
  username: admin
  password: Admin@12345

api_keys:
  tenant-a: secret-a
  tenant-b: secret-b
```

| Key / Env var | Default | Description |
|---|---|---|
| `listen_addr` / `LISTEN_ADDR` | `:8081` | HTTP listen address |
| `hot_retention_days` / `HOT_RETENTION_DAYS` | `7` | Days considered "hot" (served from OpenSearch) |
| `opensearch.addresses` / `OPENSEARCH_ADDRESSES` | `http://localhost:9200` | OpenSearch endpoint(s) |
| `opensearch.username` / `OPENSEARCH_USERNAME` | `admin` | OpenSearch username |
| `opensearch.password` / `OPENSEARCH_PASSWORD` | `Admin@12345` | OpenSearch password |
| `api_keys.<tenant>` | — | Map of tenantID → API key |

---

### tail-service

Config file: `tail-service/config.yml`

```yaml
listen_addr: ":8082"
max_active_sessions: 500

kafka:
  brokers:
    - localhost:9092
  consumer_group: tail-service
  topics:
    logs_normalized: logs-normalized
```

| Key / Env var | Default | Description |
|---|---|---|
| `listen_addr` / `LISTEN_ADDR` | `:8082` | HTTP listen address |
| `max_active_sessions` / `MAX_ACTIVE_SESSIONS` | `500` | Max concurrent SSE connections |
| `kafka.brokers` / `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker list |
| `kafka.consumer_group` / `KAFKA_CONSUMER_GROUP` | `tail-service` | Consumer group ID |
| `kafka.topics.logs_normalized` / `KAFKA_TOPICS_LOGS_NORMALIZED` | `logs-normalized` | Topic to stream entries from |

---

### alert-engine

Config file: `alert-engine/config.yml`

```yaml
kafka:
  brokers:
    - localhost:9092
  consumer_group: alert-engine
  topics:
    logs_normalized: logs-normalized

rules:
  - name: high-error-rate
    tenant_id: "*"          # "*" matches any tenant
    level: ERROR
    threshold: 100
    window_seconds: 300
    webhook: http://localhost:9000/alerts
```

| Key / Env var | Default | Description |
|---|---|---|
| `kafka.brokers` / `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker list |
| `kafka.consumer_group` / `KAFKA_CONSUMER_GROUP` | `alert-engine` | Consumer group ID |
| `kafka.topics.logs_normalized` / `KAFKA_TOPICS_LOGS_NORMALIZED` | `logs-normalized` | Topic to evaluate rules against |

**Rule fields:**

| Field | Description |
|---|---|
| `name` | Rule identifier (used in alert payload) |
| `tenant_id` | Tenant to match; `"*"` matches any |
| `level` | Log level filter (`ERROR`, `WARN`, etc.); empty = any |
| `message_contains` | Substring filter on message; empty = any |
| `threshold` | Number of matching events in the window to fire |
| `window_seconds` | Tumbling window size in seconds |
| `webhook` | URL to POST alert notification to |

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

The `store/kafka/kafka` package defines `Producer` and `Consumer` interfaces along with the `Message` type. These are the canonical types shared across all services. The services currently use `StubProducer` / `StubConsumer` (stdout logging) so they compile and run without a Kafka cluster. Replace these in each service's `main.go` with a real driver — a franz-go implementation is already provided in `store/kafka/producer` and `store/kafka/consumer`:

- [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go)
- [segmentio/kafka-go](https://github.com/segmentio/kafka-go)
- [twmb/franz-go](https://github.com/twmb/franz-go) ✓ already implemented in `store/kafka`

Similarly, `query-api` stubs OpenSearch calls. Replace `queryHot` / `queryCold` with real [opensearch-go](https://github.com/opensearch-project/opensearch-go) calls.

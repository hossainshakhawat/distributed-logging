FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.work go.work.sum* ./
COPY shared/ ./shared/
COPY log-agent/ ./log-agent/
RUN cd log-agent && go build -o /bin/log-agent ./cmd/agent

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /bin/log-agent /app/log-agent
COPY log-agent/config.yml /app/config.yml
ENTRYPOINT ["/app/log-agent"]

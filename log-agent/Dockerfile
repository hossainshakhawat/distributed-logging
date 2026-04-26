FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.work go.work.sum* ./
COPY shared/ ./shared/
COPY log-agent/ ./log-agent/
RUN cd log-agent && go build -o /bin/log-agent ./cmd/agent

FROM alpine:3.19
COPY --from=builder /bin/log-agent /usr/local/bin/log-agent
ENTRYPOINT ["log-agent"]

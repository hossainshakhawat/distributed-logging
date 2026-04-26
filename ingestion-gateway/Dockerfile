FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.work go.work.sum* ./
COPY shared/ ./shared/
COPY ingestion-gateway/ ./ingestion-gateway/
RUN cd ingestion-gateway && go build -o /bin/ingestion-gateway ./cmd/gateway

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /bin/ingestion-gateway /app/ingestion-gateway
COPY ingestion-gateway/config.yml /app/config.yml
EXPOSE 8080
ENTRYPOINT ["/app/ingestion-gateway"]

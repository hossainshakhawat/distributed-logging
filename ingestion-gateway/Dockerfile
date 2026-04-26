FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.work go.work.sum* ./
COPY shared/ ./shared/
COPY ingestion-gateway/ ./ingestion-gateway/
RUN cd ingestion-gateway && go build -o /bin/ingestion-gateway ./cmd/gateway

FROM alpine:3.19
COPY --from=builder /bin/ingestion-gateway /usr/local/bin/ingestion-gateway
EXPOSE 8080
ENTRYPOINT ["ingestion-gateway"]

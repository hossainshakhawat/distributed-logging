FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.work go.work.sum* ./
COPY shared/ ./shared/
COPY query-api/ ./query-api/
RUN cd query-api && go build -o /bin/query-api ./cmd/api

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /bin/query-api /app/query-api
COPY query-api/config.yml /app/config.yml
EXPOSE 8081
ENTRYPOINT ["/app/query-api"]

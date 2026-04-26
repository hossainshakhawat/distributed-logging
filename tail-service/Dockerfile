FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.work go.work.sum* ./
COPY shared/ ./shared/
COPY tail-service/ ./tail-service/
RUN cd tail-service && go build -o /bin/tail-service ./cmd/tail

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /bin/tail-service /app/tail-service
COPY tail-service/config.yml /app/config.yml
EXPOSE 8082
ENTRYPOINT ["/app/tail-service"]

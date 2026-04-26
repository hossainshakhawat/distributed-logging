FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.work go.work.sum* ./
COPY shared/ ./shared/
COPY stream-processor/ ./stream-processor/
RUN cd stream-processor && go build -o /bin/stream-processor ./cmd/processor

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /bin/stream-processor /app/stream-processor
COPY stream-processor/config.yml /app/config.yml
ENTRYPOINT ["/app/stream-processor"]

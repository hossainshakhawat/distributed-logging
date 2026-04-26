FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.work go.work.sum* ./
COPY shared/ ./shared/
COPY stream-processor/ ./stream-processor/
RUN cd stream-processor && go build -o /bin/stream-processor ./cmd/processor

FROM alpine:3.19
COPY --from=builder /bin/stream-processor /usr/local/bin/stream-processor
ENTRYPOINT ["stream-processor"]

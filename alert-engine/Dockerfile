FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.work go.work.sum* ./
COPY shared/ ./shared/
COPY alert-engine/ ./alert-engine/
RUN cd alert-engine && go build -o /bin/alert-engine ./cmd/alert

FROM alpine:3.19
COPY --from=builder /bin/alert-engine /usr/local/bin/alert-engine
ENTRYPOINT ["alert-engine"]

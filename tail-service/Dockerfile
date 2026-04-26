FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.work go.work.sum* ./
COPY shared/ ./shared/
COPY tail-service/ ./tail-service/
RUN cd tail-service && go build -o /bin/tail-service ./cmd/tail

FROM alpine:3.19
COPY --from=builder /bin/tail-service /usr/local/bin/tail-service
EXPOSE 8082
ENTRYPOINT ["tail-service"]

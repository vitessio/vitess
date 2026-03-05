FROM golang:1.26 AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY cmd/entrypoint ./cmd/entrypoint
COPY cmd/healthcheck ./cmd/healthcheck
COPY cmd/sleep ./cmd/sleep

RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -race -o /out/antithesis-entrypoint ./cmd/entrypoint
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -race -o /out/antithesis-healthcheck ./cmd/healthcheck
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -race -o /out/antithesis-sleep ./cmd/sleep

FROM docker.io/library/debian:bookworm-slim

COPY --from=builder /out/antithesis-entrypoint /antithesis-entrypoint
COPY --from=builder /out/antithesis-healthcheck /opt/antithesis/test/v1/main/eventually_vitess_health
COPY --from=builder /out/antithesis-sleep /opt/antithesis/test/v1/main/serial_driver_sleep

ENTRYPOINT ["/antithesis-entrypoint"]

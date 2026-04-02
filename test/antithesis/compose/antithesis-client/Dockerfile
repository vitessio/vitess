FROM golang:1.26@sha256:7fd3360ef8b48094d824850c36831d84a6004103e280b9bc9203538519b0db88 AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY cmd/entrypoint ./cmd/entrypoint
COPY cmd/healthcheck ./cmd/healthcheck
COPY cmd/sleep ./cmd/sleep

RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -race -o /out/antithesis-entrypoint ./cmd/entrypoint
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -race -o /out/antithesis-healthcheck ./cmd/healthcheck
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -race -o /out/antithesis-sleep ./cmd/sleep

FROM docker.io/library/debian:bookworm-slim@sha256:8af0e5095f9964007f5ebd11191dfe52dcb51bf3afa2c07f055fc5451b78ba0e

RUN apt-get update && apt-get install -y python3 python3-pip && rm -rf /var/lib/apt/lists/*
RUN pip3 install --no-cache-dir --break-system-packages mysql-connector-python==9.6.0 antithesis==0.2.0

COPY --from=builder /out/antithesis-entrypoint /antithesis-entrypoint
COPY --from=builder /out/antithesis-healthcheck /opt/antithesis/test/v1/main/eventually_vitess_health
COPY --from=builder /out/antithesis-sleep /opt/antithesis/test/v1/main/serial_driver_sleep

COPY ./helper.py /opt/antithesis/resources/helper.py
COPY crud/ /opt/antithesis/test/v1/crud
COPY bank-test/ /opt/antithesis/test/v1/bank-test

RUN mkdir -p /opt/antithesis/catalog \
    && ln -s /opt/antithesis/test /opt/antithesis/catalog/test \
    && ln -s /opt/antithesis/resources /opt/antithesis/catalog/resources

ENTRYPOINT ["/antithesis-entrypoint"]

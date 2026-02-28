FROM golang:1.26 AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY cmd/entrypoint ./cmd/entrypoint
COPY cmd/healthcheck ./cmd/healthcheck

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/antithesis-entrypoint ./cmd/entrypoint
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/antithesis-healthcheck ./cmd/healthcheck

FROM gcr.io/distroless/static:nonroot

COPY --from=builder /out/antithesis-entrypoint /antithesis-entrypoint
COPY --from=builder /out/antithesis-healthcheck /opt/antithesis/test/v1/main/eventually_vitess_health

ENTRYPOINT ["/antithesis-entrypoint"]

FROM golang:1.12.5 AS build
COPY . /go/src/vitess.io/vitess/examples/are-you-alive
RUN CGO_ENABLED=0 go install vitess.io/vitess/examples/are-you-alive/cmd/are-you-alive

FROM debian:stretch-slim AS runtime
COPY --from=build /go/bin/are-you-alive /go/bin/are-you-alive
ENTRYPOINT ["/go/bin/are-you-alive"]

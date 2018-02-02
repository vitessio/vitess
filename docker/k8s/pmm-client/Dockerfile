FROM vitess/base AS base

FROM debian:stretch-slim

# Copy CA certs for https calls
COPY --from=base /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

RUN apt-get update && \
    apt-get upgrade -qq && \
    apt-get install wget -qq --no-install-recommends && \
    wget https://www.percona.com/redir/downloads/pmm-client/1.6.1/binary/debian/stretch/x86_64/pmm-client_1.6.1-1.stretch_amd64.deb && \
    dpkg -i pmm-client_1.6.1-1.stretch_amd64.deb && \
    rm pmm-client_1.6.1-1.stretch_amd64.deb && \
    apt-get purge wget -qq && \
    apt-get autoremove -qq && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

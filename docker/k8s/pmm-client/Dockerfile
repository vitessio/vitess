FROM vitess/k8s AS k8s

FROM debian:stretch-slim

RUN apt-get update && \
   apt-get upgrade -qq && \
   apt-get install procps wget ca-certificates -qq --no-install-recommends && \
   wget https://www.percona.com/redir/downloads/pmm-client/1.17.0/binary/debian/stretch/x86_64/pmm-client_1.17.0-1.stretch_amd64.deb && \
   dpkg -i pmm-client_1.17.0-1.stretch_amd64.deb && \
   rm pmm-client_1.17.0-1.stretch_amd64.deb && \
   apt-get purge wget ca-certificates -qq && \
   apt-get autoremove -qq && \
   apt-get clean && \
   rm -rf /var/lib/apt/lists/*

# Copy CA certs for https calls
COPY --from=k8s /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

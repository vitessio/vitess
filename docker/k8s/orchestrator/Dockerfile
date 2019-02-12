FROM vitess/k8s AS k8s

FROM debian:stretch-slim

RUN apt-get update && \
   apt-get upgrade -qq && \
   apt-get install wget ca-certificates jq -qq --no-install-recommends && \
   wget https://github.com/github/orchestrator/releases/download/v3.0.14/orchestrator_3.0.14_amd64.deb && \
   dpkg -i orchestrator_3.0.14_amd64.deb && \
   rm orchestrator_3.0.14_amd64.deb && \
   apt-get purge wget -qq && \
   apt-get autoremove -qq && \
   apt-get clean && \
   rm -rf /var/lib/apt/lists/*

# Copy vtctlclient to be used to notify
COPY --from=k8s /vt/bin/vtctlclient /usr/bin/

WORKDIR /usr/local/orchestrator
CMD ["./orchestrator", "--config=/conf/orchestrator.conf.json", "http"]

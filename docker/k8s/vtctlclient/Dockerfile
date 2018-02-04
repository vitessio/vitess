FROM vitess/base AS base
FROM debian:stretch-slim
COPY --from=base /vt/bin/vtctlclient /usr/bin/
RUN apt-get update && \
    apt-get upgrade -qq && \
    apt-get install jq -qq --no-install-recommends && \
    apt-get autoremove && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
CMD ["/usr/bin/vtctlclient"]

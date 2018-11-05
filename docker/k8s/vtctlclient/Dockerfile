FROM vitess/k8s AS k8s

FROM debian:stretch-slim

RUN apt-get update && \
   apt-get upgrade -qq && \
   apt-get install jq -qq --no-install-recommends && \
   apt-get autoremove && \
   apt-get clean && \
   rm -rf /var/lib/apt/lists/*

COPY --from=k8s /vt/bin/vtctlclient /usr/bin/

# add vitess user/group and add permissions
RUN groupadd -r --gid 2000 vitess && \
   useradd -r -g vitess --uid 1000 vitess && \
   chown -R vitess:vitess /vt && \
   chown -R vitess:vitess /vtdataroot
    
CMD ["/usr/bin/vtctlclient"]

FROM vitess/k8s AS k8s

FROM debian:stretch-slim

# TODO: remove when https://github.com/vitessio/vitess/issues/3553 is fixed
RUN apt-get update && \
   apt-get upgrade -qq && \
   apt-get install wget mysql-client jq -qq --no-install-recommends && \
   apt-get autoremove && \
   apt-get clean && \
   rm -rf /var/lib/apt/lists/*

# Set up Vitess environment (just enough to run pre-built Go binaries)
ENV VTROOT /vt
ENV VTDATAROOT /vtdataroot

# Prepare directory structure.
RUN mkdir -p /vt/bin && mkdir -p /vtdataroot

# Copy binaries
COPY --from=k8s /vt/bin/vttablet /vt/bin/
COPY --from=k8s /vt/bin/vtctlclient /vt/bin/

# Copy certs to allow https calls
COPY --from=k8s /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

# add vitess user/group and add permissions
RUN groupadd -r --gid 2000 vitess && \
   useradd -r -g vitess --uid 1000 vitess && \
   chown -R vitess:vitess /vt && \
   chown -R vitess:vitess /vtdataroot

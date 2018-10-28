FROM vitess/k8s AS k8s

FROM debian:stretch-slim

# Set up Vitess environment (just enough to run pre-built Go binaries)
ENV VTROOT /vt
ENV VTDATAROOT /vtdataroot

# Prepare directory structure.
RUN mkdir -p /vt/bin

# Copy binaries
COPY --from=k8s /vt/bin/vttablet /vt/bin/
COPY --from=k8s /vt/bin/vtctlclient /vt/bin/

# Copy certs to allow https calls
COPY --from=k8s /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

# TODO: remove when https://github.com/vitessio/vitess/issues/3553 is fixed
RUN apt-get update && \
   apt-get upgrade -qq && \
   apt-get install mysql-client jq -qq --no-install-recommends && \
   apt-get autoremove && \
   apt-get clean && \
   rm -rf /var/lib/apt/lists/*
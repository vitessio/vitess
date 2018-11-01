FROM vitess/k8s AS k8s

FROM debian:stretch-slim

RUN apt-get update && \
   apt-get upgrade -qq && \
   apt-get install busybox -qq --no-install-recommends && \
   apt-get autoremove && \
   apt-get clean && \
   rm -rf /var/lib/apt/lists/*

# Set up Vitess environment (just enough to run pre-built Go binaries)
ENV VTROOT /vt
ENV VTDATAROOT /vtdataroot

# Prepare directory structure.
RUN mkdir -p /vt/bin && \
   mkdir -p /vt/config

# Copy binaries
COPY --from=k8s /vt/bin/mysqlctld /vt/bin/

# Copy certs to allow https calls
COPY --from=k8s /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

# copy vitess config
COPY --from=k8s /vt/config /vt/config

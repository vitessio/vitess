FROM vitess/k8s AS k8s

FROM debian:stretch-slim

# Set up Vitess environment (just enough to run pre-built Go binaries)
ENV VTROOT /vt

# Prepare directory structure.
RUN mkdir -p /vt/bin && mkdir -p /vtdataroot

# Copy certs to allow https calls
COPY --from=k8s /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

# Copy binaries
COPY --from=k8s /vt/bin/vtworker /vt/bin/

# add vitess user/group and add permissions
RUN groupadd -r --gid 2000 vitess && \
   useradd -r -g vitess --uid 1000 vitess && \
   chown -R vitess:vitess /vt && \
   chown -R vitess:vitess /vtdataroot
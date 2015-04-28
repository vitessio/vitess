# This image is only meant to be built from within the build.sh script.
FROM debian:wheezy

# Install dependencies
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    curl \
 && rm -rf /var/lib/apt/lists/*

# Copy binaries (placed by build.sh)
COPY base/etcd /usr/bin/

#!/bin/bash

# This is a script that gets run as part of the Dockerfile build
# to install dependencies for the vitess/mini image.
#
# Usage: install_mini_dependencies.sh

set -euo pipefail

# Install etcd
ETCD_VER=v3.4.9
DOWNLOAD_URL=https://storage.googleapis.com/etcd

curl -k -L ${DOWNLOAD_URL}/${ETCD_VER}/etcd-${ETCD_VER}-linux-amd64.tar.gz -o /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz
mkdir -p /var/opt/etcd
sudo tar xzvf /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz -C /var/opt/etcd --strip-components=1
rm -f /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz

mkdir -p /var/run/etcd && chown -R vitess:vitess /var/run/etcd

# Install orchestrator
curl -k -L https://github.com/openark/orchestrator/releases/download/v3.2.2/orchestrator_3.2.2_amd64.deb -o /tmp/orchestrator.deb
dpkg -i /tmp/orchestrator.deb
rm -f /tmp/orchestrator.deb
cp /usr/local/orchestrator/resources/bin/orchestrator-client /usr/bin

# Clean up files we won't need in the final image.
rm -rf /var/lib/apt/lists/*

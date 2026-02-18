#!/bin/bash

# This is a script that gets run as part of the Dockerfile build
# to install dependencies for the vitess/mini image.
#
# Usage: install_mini_dependencies.sh

set -euo pipefail

# Install etcd
ETCD_VER=v3.6.7
ETCD_SHA256="cf8af880c5a01ee5363cefa14a3e0cb7e5308dcf4ed17a6973099c9a7aee5a9a"
DOWNLOAD_URL=https://storage.googleapis.com/etcd

curl -k -L ${DOWNLOAD_URL}/${ETCD_VER}/etcd-${ETCD_VER}-linux-amd64.tar.gz -o /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz
echo "${ETCD_SHA256}  /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz" | sha256sum -c -
mkdir -p /var/opt/etcd
sudo tar xzvf /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz -C /var/opt/etcd --strip-components=1
rm -f /tmp/etcd-${ETCD_VER}-linux-amd64.tar.gz

mkdir -p /var/run/etcd && chown -R vitess:vitess /var/run/etcd

# Install orchestrator
ORCHESTRATOR_SHA256="e90fa66a37c8d509e7d4b1c3a4118fd8c8bc8d8b856fa183ddadf31f11a1f3f7"
curl -k -L https://github.com/openark/orchestrator/releases/download/v3.2.2/orchestrator_3.2.2_amd64.deb -o /tmp/orchestrator.deb
echo "${ORCHESTRATOR_SHA256}  /tmp/orchestrator.deb" | sha256sum -c -
dpkg -i /tmp/orchestrator.deb
rm -f /tmp/orchestrator.deb
cp /usr/local/orchestrator/resources/bin/orchestrator-client /usr/bin

# Clean up files we won't need in the final image.
rm -rf /var/lib/apt/lists/*

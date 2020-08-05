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

# Install gh-ost
curl -k -L https://github.com/openark/gh-ost/releases/download/v1.1.0/gh-ost-binary-linux-20200805092717.tar.gz -o /tmp/gh-ost.tar.gz
(cd /tmp/ && tar xzf gh-ost.tar.gz)
cp /tmp/gh-ost /usr/bin

# Clean up files we won't need in the final image.
rm -rf /var/lib/apt/lists/*

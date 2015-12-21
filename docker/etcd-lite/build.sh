#!/bin/bash

# This is the script to build the vitess/etcd-lite Docker image by extracting
# the pre-built binaries from a vitess/etcde image.

version="v2.0.13"

set -e

# Build a fresh base vitess/etcd image
(cd ../etcd ; sudo docker build -t vitess/etcd:$version .)

# Extract files from vitess/etcd image
mkdir base
sudo docker run -ti --rm -v $PWD/base:/base -u root vitess/etcd:$version bash -c 'cp -R /go/bin/* /base/'

# Build vitess/etcd-lite image
sudo docker build -t vitess/etcd:$version-lite .

# Clean up temporary files
sudo rm -rf base

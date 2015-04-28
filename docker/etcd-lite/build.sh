#!/bin/bash

# This is the script to build the vitess/etcd-lite Docker image by extracting
# the pre-built binaries from a vitess/etcde image.

# Extract files from vitess/etcd image
mkdir base
sudo docker run -ti --rm -v $PWD/base:/base -u root vitess/etcd:v0.4.6 bash -c 'cp -R /go/bin/etcd /base/'

# Build vitess/etcd-lite image
sudo docker build -t vitess/etcd:v0.4.6-lite .

# Clean up temporary files
sudo rm -rf base

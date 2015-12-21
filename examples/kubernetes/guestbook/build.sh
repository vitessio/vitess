#!/bin/bash

# This is a script to build the vitess/guestbook Docker image.
# It must be run from within a bootstrapped vitess tree, after dev.env.

set -e

mkdir tmp tmp/pkg tmp/lib
cp extract.sh tmp/
chmod -R 777 tmp

# We also need the grpc library.
docker run --rm -v $PWD/tmp:/out vitess/base bash /out/extract.sh

# Build the Docker image.
docker build -t vitess/guestbook .

# Clean up.
docker run --rm -v $PWD/tmp:/out vitess/base bash -c 'rm -rf /out/pkg/* /out/lib/*'
rm -rf tmp

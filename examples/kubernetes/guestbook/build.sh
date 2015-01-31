#!/bin/bash

# This is a script to build the vitess/guestbook Docker image.
# It must be run from within a bootstrapped vitess tree, after dev.env.

set -e

mkdir tmp

# Collect all the local Python libs we need.
cp -R $VTTOP/py/* tmp/
for pypath in $(find $VTROOT/dist -name site-packages); do
  cp -R $pypath/* tmp/
done

# Build the Docker image.
docker build -t vitess/guestbook .

# Clean up.
rm -rf tmp

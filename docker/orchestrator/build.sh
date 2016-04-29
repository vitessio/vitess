#!/bin/bash

set -e

tmpdir=`mktemp -d`

script="go get github.com/youtube/vitess/go/cmd/vtctlclient && \
  git clone https://github.com/enisoc/orchestrator.git src/github.com/outbrain/orchestrator && \
  go install github.com/outbrain/orchestrator/go/cmd/orchestrator"

echo "Building orchestrator..."
docker run -ti --name=vt_orc_build golang:1.6 bash -c "$script"
docker cp vt_orc_build:/go/bin/orchestrator $tmpdir
docker cp vt_orc_build:/go/bin/vtctlclient $tmpdir
docker cp vt_orc_build:/go/src/github.com/outbrain/orchestrator/resources $tmpdir
docker rm vt_orc_build

echo "Building Docker image..."
cp Dockerfile orchestrator.conf.json $tmpdir
(cd $tmpdir && docker build -t vitess/orchestrator .)

# Clean up
rm -r $tmpdir


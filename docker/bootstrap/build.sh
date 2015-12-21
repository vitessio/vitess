#!/bin/bash

flavor=$1

if [[ -z "$flavor" ]]; then
  echo "Flavor must be specified as first argument."
  exit 1
fi

if [[ ! -f bootstrap.sh ]]; then
  echo "This script should be run from the root of the Vitess source tree - e.g. ~/src/github.com/youtube/vitess"
  exit 1
fi

# To avoid AUFS permission issues, files must allow access by "other"
chmod -R o=g *

docker build --no-cache -f docker/bootstrap/Dockerfile.$flavor -t vitess/bootstrap:$flavor .

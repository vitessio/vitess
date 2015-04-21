#!/bin/bash

flavor=$1
cmd=$2

if [[ -z "$flavor" ]]; then
  echo "Flavor must be specified as first argument."
  exit 1
fi

if [[ -z "$cmd" ]]; then
  cmd=bash
fi

if [[ ! -f bootstrap.sh ]]; then
  echo "This script should be run from the root of the Vitess source tree - e.g. ~/src/github.com/youtube/vitess"
  exit 1
fi

# To avoid AUFS permission issues, files must allow access by "other"
chmod -R o=g *

args="-ti --rm -e USER=vitess -v /dev/log:/dev/log"
args="$args -v $PWD:/tmp/src"

# Mount in host VTDATAROOT if one exists, since it might be a RAM disk or SSD.
if [[ -n "$VTDATAROOT" ]]; then
  hostdir=`mktemp -d --tmpdir=$VTDATAROOT test-XXX`
  testid=`basename $hostdir`

  chmod 777 $hostdir

  echo "Mounting host dir $hostdir as VTDATAROOT"
  args="$args -v $hostdir:/vt/vtdataroot --name=$testid -h $testid"
else
  args="$args -h test"
fi

# Run tests
echo "Running tests in vitess/bootstrap:$flavor image..."
docker run $args vitess/bootstrap:$flavor \
  bash -c "rm -rf * && cp -R /tmp/src/* . && rm -rf Godeps/_workspace/pkg && $cmd"

# Clean up host dir mounted VTDATAROOT
if [[ -n "$hostdir" ]]; then
  rm -rf $hostdir
fi

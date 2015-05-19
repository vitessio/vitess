#!/bin/bash

flavor=$1
cmd=$2
args=

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

# To avoid AUFS permission issues, files must allow access by "other" (permissions rX required).
# Mirror permissions to "other" from the owning group (for which we assume it has at least rX permissions).
chmod -R o=g .

args="$args --rm -e USER=vitess -v /dev/log:/dev/log"
args="$args -v $PWD:/tmp/src"

# Mount in host VTDATAROOT if one exists, since it might be a RAM disk or SSD.
if [[ -n "$VTDATAROOT" ]]; then
  hostdir=`mktemp -d --tmpdir=$VTDATAROOT test-XXX`
  testid=`basename $hostdir`

  chmod 777 $hostdir

  echo "Mounting host dir $hostdir as VTDATAROOT"
  args="$args -v $hostdir:/vt/vtdataroot --name=$testid -h $testid"
else
  testid=test-$$
  args="$args --name=$testid -h $testid"
fi

# Run tests
echo "Running tests in vitess/bootstrap:$flavor image..."
bashcmd="rm -rf * && cp -R /tmp/src/* . && rm -rf Godeps/_workspace/pkg && $cmd"

if tty -s; then
  # interactive shell
  docker run -ti $args vitess/bootstrap:$flavor bash -c "$bashcmd"
  exitcode=$?
else
  # non-interactive shell (kill child on signal)
  trap 'docker rm -f $testid 2>/dev/null' SIGTERM SIGINT
  docker run $args vitess/bootstrap:$flavor bash -c "$bashcmd" &
  wait $!
  exitcode=$?
fi

# Clean up host dir mounted VTDATAROOT
if [[ -n "$hostdir" ]]; then
  # Use Docker user to clean up first, to avoid permission errors.
  docker run $args vitess/bootstrap:$flavor bash -c 'rm -rf /vt/vtdataroot/*'
  rm -rf $hostdir
fi

exit $exitcode

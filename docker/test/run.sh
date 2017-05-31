#!/bin/bash

# Copyright 2017 Google Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

args="$args -v /dev/log:/dev/log"
args="$args -v $PWD:/tmp/src"

# Share maven dependency cache so they don't have to be redownloaded every time.
mkdir -p /tmp/mavencache
chmod 777 /tmp/mavencache
args="$args -v /tmp/mavencache:/home/vitess/.m2"

# Mount in host VTDATAROOT if one exists, since it might be a RAM disk or SSD.
if [[ -n "$VTDATAROOT" ]]; then
  hostdir=`mktemp -d $VTDATAROOT/test-XXX`
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
bashcmd="mv php/vendor /vt/dist/php-vendor && mv vendor /vt/dist/go-vendor && rm -rf * && mkdir php && mv /vt/dist/php-vendor php/vendor && mv /vt/dist/go-vendor vendor && cp -R /tmp/src/* . && rm -rf Godeps/_workspace/pkg && $cmd"

if tty -s; then
  # interactive shell
  docker run -ti $args vitess/bootstrap:$flavor bash -c "$bashcmd"
  exitcode=$?
else
  # non-interactive shell (kill child on signal)
  trap 'docker kill $testid &>/dev/null' SIGTERM SIGINT
  docker run $args vitess/bootstrap:$flavor bash -c "$bashcmd" &
  wait $!
  exitcode=$?
fi

# Clean up host dir mounted VTDATAROOT
if [[ -n "$hostdir" ]]; then
  # Use Docker user to clean up first, to avoid permission errors.
  docker run --name=rm_$testid -v $hostdir:/vt/vtdataroot vitess/bootstrap:$flavor bash -c 'rm -rf /vt/vtdataroot/*'
  docker rm -f rm_$testid &>/dev/null
  rm -rf $hostdir
fi

# Delete the container
docker rm -f $testid &>/dev/null

exit $exitcode

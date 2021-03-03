#!/bin/bash

# Copyright 2019 The Vitess Authors.
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

# Script to run arbitrary commands within our bootstrap Docker images.
#
# NOTE: If your goal is to run a test within an image, please use our test
#       runner "test.go" in the root directory instead. test.go will
#       internally use this script to launch the test for you.

# If you still think that you want to use this script, read on.
#
# The script has two modes it can run in:
# - run_test:     Run the test cmd in the given Docker image ("flavor").
# - create_cache: Create a new Docker image after copying the source and
#                 running "make build". Such an image can be reused in
#                 future test invocations via --use_docker_cache <image>.
#
# Examples:
#  a) Start an interactive shell within the Docker image.
#  $ docker/test/run.sh mysql57 bash
#
#  b) Build the code and run a test.
#  $ docker/test/run.sh mysql57 "make build && ./test/keyrange_test.py -v"
#
#  c) Cache the output of the command e.g. cache "make build" as we do for Travis CI.
#  $ docker/test/run.sh --create_docker_cache vitess/bootstrap:rm_mysql57_test_cache_do_NOT_push mysql57 "make build"
#
#  d) Run the test using a cache image.
#  $ docker/test/run.sh --use_docker_cache vitess/bootstrap:rm_mysql57_test_cache_do_NOT_push mysql57 "./test/keyrange_test.py -v"


# Functions.
# Helper to append additional commands via "&&".
function append_cmd() {
  local cmd="$1"
  local append="$2"
  if [[ -n "$cmd" ]]; then
    cmd+=" && "
  fi
  cmd+="$append"
  echo "$cmd"
}

# Variables.
# Default to "run_test" mode unless the --create_docker_cache flag is found.
mode="run_test"


# Main.
# Parse non-positional flags.
while true ; do
  case "$1" in
    --create_docker_cache)
      case "$2" in
          "")
            echo "ERROR: --create_docker_cache requires the name of the image as second parameter"
            exit 1
            ;;
          *)
            mode="create_cache"
            cache_image=$2
            shift 2
            ;;
      esac ;;
    --use_docker_cache)
        case "$2" in
            "")
              echo "ERROR: --use_docker_cache requires the name of the image as second parameter"
              exit 1
              ;;
            *)
              existing_cache_image=$2
              shift 2
              ;;
        esac ;;
    -*)
      echo "ERROR: Unrecognized flag: $1"
      exit 1
      ;;
    *)
      # No more non-positional flags.
      break
      ;;
  esac
done
# Positional flags.
flavor=$1
version=${2:-0}
cmd=$3
args=

if [[ -z "$flavor" ]]; then
  echo "Flavor must be specified as first argument."
  exit 1
fi

if [[ -z "$cmd" ]]; then
  cmd=bash
fi

if [[ ! -f bootstrap.sh ]]; then
  echo "This script should be run from the root of the Vitess source tree - e.g. ~/src/vitess.io/vitess"
  exit 1
fi

image=vitess/bootstrap:$version-$flavor
if [[ -n "$existing_cache_image" ]]; then
  image=$existing_cache_image
fi

# To avoid AUFS permission issues, files must allow access by "other" (permissions rX required).
# Mirror permissions to "other" from the owning group (for which we assume it has at least rX permissions).
chmod -R o=g .

# This is required by the vtctld_web_test.py test.
# Otherwise, /usr/bin/chromium will crash with the error:
# "Failed to move to new namespace: PID namespaces supported, Network namespace supported, but failed: errno = Operation not permitted"
args="$args --cap-add=SYS_ADMIN"

args="$args -v /dev/log:/dev/log"
args="$args -v $PWD:/tmp/src"

# Share maven dependency cache so they don't have to be redownloaded every time.
mkdir -p /tmp/mavencache
chmod 777 /tmp/mavencache
args="$args -v /tmp/mavencache:/home/vitess/.m2"

# Add in the vitess user
args="$args --user vitess"
args="$args -v $PWD/test/bin:/tmp/bin"

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
case "$mode" in
  "run_test") echo "Running tests in $image image..." ;;
  "create_cache") echo "Creating cache image $cache_image ..." ;;
esac

bashcmd=""

if [[ -z "$existing_cache_image" ]]; then

  # Construct "cp" command to copy the source code.
  bashcmd=$(append_cmd "$bashcmd" "cp -R /tmp/src/!(vtdataroot|dist|bin|lib|vthook) . && cp -R /tmp/src/.git .")

fi

# Reset the environment if this was an old bootstrap. We can detect this from VTTOP presence.
bashcmd=$(append_cmd "$bashcmd" "export VTROOT=/vt/src/vitess.io/vitess")
bashcmd=$(append_cmd "$bashcmd" "export VTDATAROOT=/vt/vtdataroot")
bashcmd=$(append_cmd "$bashcmd" "export EXTRA_BIN=/tmp/bin")

bashcmd=$(append_cmd "$bashcmd" "mkdir -p dist; mkdir -p bin; mkdir -p lib; mkdir -p vthook")
bashcmd=$(append_cmd "$bashcmd" "rm -rf /vt/dist; ln -s /vt/src/vitess.io/vitess/dist /vt/dist")
bashcmd=$(append_cmd "$bashcmd" "rm -rf /vt/bin; ln -s /vt/src/vitess.io/vitess/bin /vt/bin")
bashcmd=$(append_cmd "$bashcmd" "rm -rf /vt/lib; ln -s /vt/src/vitess.io/vitess/lib /vt/lib")
bashcmd=$(append_cmd "$bashcmd" "rm -rf /vt/vthook; ln -s /vt/src/vitess.io/vitess/vthook /vt/vthook")

# Maven was setup in /vt/dist, may need to reinstall it.
bashcmd=$(append_cmd "$bashcmd" "echo 'Checking if mvn needs installing...'; if [[ ! \$(command -v mvn) ]]; then echo 'install maven'; curl -sL --connect-timeout 10 --retry 3 http://www-us.apache.org/dist/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz | tar -xz && mv apache-maven-3.3.9 /vt/dist/maven; fi; echo 'mvn check done'")

# Run bootstrap every time now
bashcmd=$(append_cmd "$bashcmd" "./bootstrap.sh")

# At last, append the user's command.
bashcmd=$(append_cmd "$bashcmd" "$cmd")

if tty -s; then
  # interactive shell
  # See above why we turn on "extglob" (extended Glob).
  docker run -ti $args $image bash -O extglob -c "$bashcmd"
  exitcode=$?
else
  # non-interactive shell (kill child on signal)
  trap 'docker kill $testid &>/dev/null' SIGTERM SIGINT
  docker run $args $image bash -O extglob -c "$bashcmd" &
  wait $!
  exitcode=$?
  trap - SIGTERM SIGINT
fi

# Clean up host dir mounted VTDATAROOT
if [[ -n "$hostdir" ]]; then
  # Use Docker user to clean up first, to avoid permission errors.
  docker run --name=rm_$testid -v $hostdir:/vt/vtdataroot $image bash -c 'rm -rf /vt/vtdataroot/*'
  docker rm -f rm_$testid &>/dev/null
  rm -rf $hostdir
fi

# If requested, create the cache image.
if [[ "$mode" == "create_cache" && $exitcode == 0 ]]; then
  msg="DO NOT PUSH: This is a temporary layer meant to persist e.g. the result of 'make build'. Never push this layer back to our official Docker Hub repository."
  docker commit -m "$msg" $testid $cache_image

  if [[  $? != 0 ]]; then
    exitcode=$?
    echo "ERROR: Failed to create Docker cache. Used command: docker commit -m '$msg' $testid $image"
  fi
fi

# Delete the container
docker rm -f $testid &>/dev/null

exit $exitcode

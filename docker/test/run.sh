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
# By default, the script copies the source tree into the bootstrap image of the
# given flavor and runs the command there. With --image <image>, it runs the
# command in <image> instead, which already contains the source tree and its
# build (see docker/test/Dockerfile).
#
# Examples:
#  a) Start an interactive shell within the Docker image.
#  $ docker/test/run.sh mysql80 bash
#
#  b) Build the code and run a test.
#  $ docker/test/run.sh mysql80 "make build && ./test/keyrange_test.py -v"
#
#  c) Run a test in an image that already contains the build.
#  $ docker/test/run.sh --image vitess-test mysql80 "./test/keyrange_test.py -v"

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

# Main.
# Parse non-positional flags.
while true; do
	case "$1" in
	--image)
		case "$2" in
		"")
			echo "ERROR: --image requires the name of the image as second parameter"
			exit 1
			;;
		*)
			prebuilt_image=$2
			shift 2
			;;
		esac
		;;
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
if [[ -n "$prebuilt_image" ]]; then
	image=$prebuilt_image
fi

# Fix permissions before copying files, to avoid AUFS bug other must have read/access permissions
chmod -R o=rx -- *

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

# Pass through TOPO environment variable if it's set
if [[ -n "$TOPO" ]]; then
	args="$args -e TOPO=$TOPO"
fi

# Pass through PACKAGES environment variable if it's set
if [[ -n "$PACKAGES" ]]; then
	args="$args -e PACKAGES=$PACKAGES"
fi

# Mount in host VTDATAROOT if one exists, since it might be a RAM disk or SSD.
if [[ -n "$VTDATAROOT" ]]; then
	hostdir=$(mktemp -d "$VTDATAROOT/test-XXX")
	testid=$(basename "$hostdir")

	chmod 777 "$hostdir"

	echo "Mounting host dir $hostdir as VTDATAROOT"
	args="$args -v $hostdir:/vt/vtdataroot --name=$testid -h $testid"
else
	testid=test-$$
	args="$args --name=$testid -h $testid"
fi

# Run tests
echo "Running tests in $image image..."

bashcmd=""

if [[ -z "$prebuilt_image" ]]; then
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

# Install Maven 3.9.12 on-demand (not in base image to avoid CVE-2024-47554)
bashcmd=$(append_cmd "$bashcmd" "if [[ ! -x dist/maven/bin/mvn ]]; then echo 'Installing Maven 3.9.12...'; curl -fsSL --connect-timeout 10 --retry 3 https://archive.apache.org/dist/maven/maven-3/3.9.12/binaries/apache-maven-3.9.12-bin.tar.gz | tar -xz && mv apache-maven-3.9.12 dist/maven && echo 'Maven installed'; fi")

# Run bootstrap every time now
bashcmd=$(append_cmd "$bashcmd" "./bootstrap.sh")

# At last, append the user's command.
bashcmd=$(append_cmd "$bashcmd" "$cmd")

if tty -s; then
	# interactive shell
	# See above why we turn on "extglob" (extended Glob).
	# shellcheck disable=SC2086
	docker run -ti $args "$image" bash -O extglob -c "$bashcmd"
	exitcode=$?
else
	# non-interactive shell (kill child on signal)
	trap 'docker kill $testid &>/dev/null' SIGTERM SIGINT
	# shellcheck disable=SC2086
	docker run $args "$image" bash -O extglob -c "$bashcmd" &
	wait $!
	exitcode=$?
	trap - SIGTERM SIGINT
fi

# Clean up host dir mounted VTDATAROOT
if [[ -n "$hostdir" ]]; then
	# Use Docker user to clean up first, to avoid permission errors.
	docker run --name="rm_$testid" -v "$hostdir":/vt/vtdataroot "$image" bash -c 'rm -rf /vt/vtdataroot/*'
	docker rm -f "rm_$testid" &>/dev/null
	rm -rf "$hostdir"
fi

# Delete the container
docker rm -f "$testid" &>/dev/null

exit $exitcode

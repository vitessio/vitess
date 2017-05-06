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

set -e

CONNECT_URL="https://saucelabs.com/downloads/sc-latest-linux.tar.gz"
CONNECT_DIR="/tmp/sauce-connect-$RANDOM"
CONNECT_DOWNLOAD="sc-latest-linux.tar.gz"

# Get Connect and start it
mkdir -p $CONNECT_DIR
cd $CONNECT_DIR
curl -s $CONNECT_URL -o $CONNECT_DOWNLOAD
mkdir sauce-connect
tar --extract --file=$CONNECT_DOWNLOAD --strip-components=1 --directory=sauce-connect
rm $CONNECT_DOWNLOAD

ARGS=""

# Set tunnel-id only on Travis, to make local testing easier.
if [ ! -z "$TRAVIS_JOB_NUMBER" ]; then
  ARGS="$ARGS --tunnel-identifier $TRAVIS_JOB_NUMBER"
fi
if [ ! -z "$BROWSER_PROVIDER_READY_FILE" ]; then
  ARGS="$ARGS --readyfile $BROWSER_PROVIDER_READY_FILE"
fi

echo "Starting Sauce Connect in the background"
sauce-connect/bin/sc $ARGS &

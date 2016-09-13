#!/bin/bash

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

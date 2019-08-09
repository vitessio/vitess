#!/usr/bin/env bash

set -e

printf "\nStopping Vitess cluster\n"

export VTROOT=/vagrant
export VTDATAROOT=/tmp/vtdata-dev
cd "$VITESS_WORKSPACE"/examples/local

./vtgate-down.sh
./vttablet-down.sh
./vtctld-down.sh
./zk-down.sh

rm -rf $VTDATAROOT

printf "\nVitess cluster stopped successfully.\n\n"

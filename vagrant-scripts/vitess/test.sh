#!/usr/bin/env bash
#
#
set -ex

ulimit -n 10000
export VT_GO_PARALLEL_VALUE=4
export VTDATAROOT=/tmp/vtdata
export VTROOT=/vagrant
export PYTHONROOT=$VTROOT/dist/grpc/usr/local/lib/python2.7/site-packages

printf "\nStarting Vitess test suite...\n"

cd "$VITESS_WORKSPACE"
# shellcheck disable=SC1091
source dev.env
# shellcheck disable=SC1091
source /vagrant/dist/grpc/usr/local/bin/activate
make site_test
rm -rf "$VTDATAROOT"

printf "\nVitess test suite completed.\n\n"

#!/bin/bash

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

pid=`cat $VTDATAROOT/tmp/vtworker.pid`
echo "Stopping vtworker..."
kill $pid


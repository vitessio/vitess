#!/bin/bash

# This is an example script that stops the instance started by vtgate-up.sh.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

# Stop vtgate.
pid=`cat $VTDATAROOT/tmp/vtgate.pid`
echo "Stopping vtgate..."
kill $pid


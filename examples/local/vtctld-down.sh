#!/bin/bash

# This is an example script that starts vtctld.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

pid=`cat $VTDATAROOT/tmp/vtctld.pid`
echo "Stopping vtctld..."
kill $pid


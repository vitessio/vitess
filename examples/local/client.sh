#!/bin/bash

# This is a wrapper script that sets up the environment for client.py.

set -e

hostname=`hostname -f`

# We expect to find zk-client-conf.json in the same folder as this script.
script_root=`dirname "${BASH_SOURCE}"`

# Set up environment.
for pkg in `find $VTROOT/dist -name site-packages`; do
  export PYTHONPATH=$pkg:$PYTHONPATH
done

export PYTHONPATH=$VTROOT/py-vtdb:$PYTHONPATH

exec env python $script_root/client.py $*

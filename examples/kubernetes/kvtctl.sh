#!/bin/bash

# This is a script that uses kubectl to figure out the address for vtctld,
# and then runs vtctlclient with that address.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

echo "Starting port forwarding to vtctld..."
start_vtctld_forward
trap stop_vtctld_forward EXIT

vtctlclient -server 127.0.0.1:$vtctld_forward_port "$@"


#!/bin/bash

# This is a script that uses kubectl to figure out the address for vtctld,
# and then runs vtctlclient with that address.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

echo "Getting vtctld address from kubectl..."
get_vtctld_addr
if [[ -z "$VTCTLD_ADDR" ]]; then
  echo "Can't find VTCTLD_ADDR."
  exit 1
fi

echo "Using $VTCTLD_ADDR"
vtctlclient -server $VTCTLD_ADDR "$@"


#!/bin/bash

# This is an example script that stops the mysqld and vttablet instances
# created by sharded-vttablet-up.sh

script_root=`dirname "${BASH_SOURCE}"`

UID_BASE=200 $script_root/vttablet-down.sh "$@"
UID_BASE=300 $script_root/vttablet-down.sh "$@"


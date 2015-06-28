#!/bin/bash

# This is an example script that tears down the vttablet deployment
# created by sharded-vttablet-up.sh.

SHARDS='-80,80-'
UID_BASE=200

source ./vttablet-down.sh

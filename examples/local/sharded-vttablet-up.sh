#!/bin/bash

# This is an example script that creates a sharded vttablet deployment.

set -e

script_root=`dirname "${BASH_SOURCE}"`

# Shard -80 contains all entries whose keyspace ID has a first byte < 0x80.
# See: http://vitess.io/overview/concepts.html#keyspace-id
SHARD=-80 UID_BASE=200 $script_root/vttablet-up.sh "$@"

# Shard 80- contains all entries whose keyspace ID has a first byte >= 0x80.
SHARD=80- UID_BASE=300 $script_root/vttablet-up.sh "$@"


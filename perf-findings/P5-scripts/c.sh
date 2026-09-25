#!/bin/bash
# Run the P5 copy of the harness as the vt user.
exec setpriv --reuid=vt --regid=vt --init-groups env HOME=/home/vt BASE=40000 SHARDS=${SHARDS:-0} REPLICAS=${REPLICAS:-2} \
  BIN=${BIN:-/home/vt/bin} DURABILITY=${DURABILITY:-none} TABLE_SIZE=${TABLE_SIZE:-100000} TABLES=${TABLES:-4} REDO_CAPACITY=${REDO_CAPACITY:-}\
  VTGATE_EXTRA_FLAGS="${VTGATE_EXTRA_FLAGS:-}" VTTABLET_EXTRA_FLAGS="${VTTABLET_EXTRA_FLAGS:-}" \
  PATH=/usr/local/bin:/usr/bin:/bin /home/vt/perf/P5/cluster.sh "$@"

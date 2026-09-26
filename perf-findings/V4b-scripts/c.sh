#!/bin/bash
# Run the V4 harness as the vt user.
exec setpriv --reuid=vt --regid=vt --init-groups env HOME=/home/vt BASE=40000 BIN=${BIN:-/home/vt/bin} \
  KEYSPACES="${KEYSPACES:-src:0 dst2:-80,80- many:0 mdst:0}" TABLES=${TABLES:-2} TABLE_SIZE=${TABLE_SIZE:-200000} \
  VTGATE_EXTRA_FLAGS="${VTGATE_EXTRA_FLAGS:-}" VTTABLET_EXTRA_FLAGS="${VTTABLET_EXTRA_FLAGS:-}" BUFPOOL=${BUFPOOL:-256M} \
  PATH=/usr/local/bin:/usr/bin:/bin /home/vt/perf/V4b/cluster.sh "$@"

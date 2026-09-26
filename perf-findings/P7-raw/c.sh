#!/bin/bash
# wrapper: run cluster.sh as vt with P7 settings (2 shards, 4 x 250k rows, BASE 40000).
exec runuser -u vt -- env BASE=40000 HOME=/home/vt BIN="${BIN:-/home/vt/bin}" SHARDS="${SHARDS:--80 80-}" TABLE_SIZE="${TABLE_SIZE:-250000}" VTGATE_EXTRA_FLAGS="${VTGATE_EXTRA_FLAGS:-}" VTTABLET_EXTRA_FLAGS="${VTTABLET_EXTRA_FLAGS:-}" GATE_ENV="${GATE_ENV:-}" TABLET_ENV="${TABLET_ENV:-}" /home/vt/perf/P7/cluster.sh "$@"

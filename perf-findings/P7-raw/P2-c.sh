#!/bin/bash
# wrapper: run cluster.sh as vt with P2 settings (BASE 30000). GATE_ENV / TABLET_ENV: space separated VAR=val.
exec runuser -u vt -- env BASE=30000 HOME=/home/vt BIN="${BIN:-/home/vt/bin}" VTGATE_EXTRA_FLAGS="${VTGATE_EXTRA_FLAGS:-}" VTTABLET_EXTRA_FLAGS="${VTTABLET_EXTRA_FLAGS:-}" GATE_ENV="${GATE_ENV:-}" TABLET_ENV="${TABLET_ENV:-}" TABLE_SIZE="${TABLE_SIZE:-100000}" /home/vt/perf/P2/cluster.sh "$@"

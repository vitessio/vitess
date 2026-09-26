#!/bin/bash
# wrapper: run cluster.sh as vt with P1 settings. GATE_ENV / TABLET_ENV: space separated VAR=val for vtgate / vttablet.
exec runuser -u vt -- env BASE=30000 HOME=/home/vt BIN="${BIN:-/home/vt/bin}" VTGATE_EXTRA_FLAGS="${VTGATE_EXTRA_FLAGS:-}" VTTABLET_EXTRA_FLAGS="${VTTABLET_EXTRA_FLAGS:-}" GATE_ENV="${GATE_ENV:-}" TABLET_ENV="${TABLET_ENV:-}" /home/vt/perf/P1/cluster.sh "$@"

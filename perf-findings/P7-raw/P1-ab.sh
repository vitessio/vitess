#!/bin/bash
# ab.sh ROUNDS "MODES" CFG1 CFG2 [CFG3...]
#   MODES: space separated list of MODE:THREADS, e.g. "ps:8 text:8 ps:32"
#   CFGn:  path to a shell snippet that sets BIN / GATE_ENV / TABLET_ENV / VTGATE_EXTRA_FLAGS / VTTABLET_EXTRA_FLAGS
# For every round and config: restart vtgate+vttablets with that config, warm up 5s, run each mode for $TIME (default 20s).
set -u
ROUNDS=$1; MODES=$2; shift 2
TIME=${TIME:-20}
for r in $(seq 1 "$ROUNDS"); do
  for cfg in "$@"; do
    name=$(basename "$cfg" .cfg)
    (
      unset BIN GATE_ENV TABLET_ENV VTGATE_EXTRA_FLAGS VTTABLET_EXTRA_FLAGS
      # shellcheck disable=SC1090
      source "$cfg"
      export BIN GATE_ENV TABLET_ENV VTGATE_EXTRA_FLAGS VTTABLET_EXTRA_FLAGS
      /home/vt/perf/P1/c.sh restart >/dev/null 2>&1
    )
    /home/vt/perf/P1/c.sh sb oltp_point_select --threads=8 --time=5 --db-ps-mode=auto run >/dev/null 2>&1
    for m in $MODES; do
      /home/vt/perf/P1/bench.sh "${m%%:*}" "${m##*:}" "$TIME" "r$r-$name"
    done
  done
done

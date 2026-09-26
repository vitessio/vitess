#!/bin/bash
# ab.sh ROUNDS FILTER CFG1 CFG2 [CFG3...]
#   FILTER: regex on suite.sh workload names (e.g. '.' for all)
#   CFGn:   shell snippet that sets BIN / GATE_ENV / TABLET_ENV / VTGATE_EXTRA_FLAGS / VTTABLET_EXTRA_FLAGS
# Per round and config: purge binlogs, restart vtgate+vttablets with that config, warm up, run the suite.
set -u
ROUNDS=$1; FILTER=$2; shift 2
for r in $(seq "${FIRST_ROUND:-1}" "$ROUNDS"); do
  for cfg in "$@"; do
    name=$(basename "$cfg" .cfg)
    /home/vt/perf/P7/purge.sh
    (
      unset BIN GATE_ENV TABLET_ENV VTGATE_EXTRA_FLAGS VTTABLET_EXTRA_FLAGS
      # shellcheck disable=SC1090
      source "$cfg"
      export BIN GATE_ENV TABLET_ENV VTGATE_EXTRA_FLAGS VTTABLET_EXTRA_FLAGS
      for _ in 1 2 3; do
        /home/vt/perf/P7/c.sh restart >/dev/null 2>&1
        mysql -h 127.0.0.1 -P 40003 -u root -e 'select 1' sbtest >/dev/null 2>&1 && break
        echo "restart failed, retrying" >&2
      done
    )
    /home/vt/perf/P7/c.sh sb oltp_point_select --threads=8 --time=5 run >/dev/null 2>&1
    /home/vt/perf/P7/suite.sh "r$r-$name" "$FILTER"
  done
done

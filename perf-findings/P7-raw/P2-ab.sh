#!/bin/bash
# ab3.sh ROUNDS MATRIXFILE CFG1 CFG2 [CFG3...]
# Like ab.sh, but matrix lines may start with VAR=value tokens that are exported for that run only
# (and appended to the workload tag), e.g. "SEQ=0 /home/vt/perf/P2/lua/insert_seq.lua text 8".
set -u
ROUNDS=$1; MATRIX=$2; shift 2
TIME=${TIME:-20}
for r in $(seq 1 "$ROUNDS"); do
  for cfg in "$@"; do
    name=$(basename "$cfg" .cfg)
    (
      unset BIN GATE_ENV TABLET_ENV VTGATE_EXTRA_FLAGS VTTABLET_EXTRA_FLAGS
      # shellcheck disable=SC1090
      source "$cfg"
      export BIN GATE_ENV TABLET_ENV VTGATE_EXTRA_FLAGS VTTABLET_EXTRA_FLAGS
      /home/vt/perf/P2/c.sh restart >/dev/null 2>&1
    )
    bash /home/vt/perf/P2/purge.sh
    /home/vt/perf/P2/c.sh sb oltp_write_only --threads=8 --time=5 --db-ps-mode=auto run >/dev/null 2>&1
    while read -r -a tok; do
      [[ ${#tok[@]} -eq 0 || ${tok[0]} == \#* ]] && continue
      envs=(); tag=""
      while [[ ${tok[0]} == *=* ]]; do envs+=("${tok[0]}"); tag+="${tok[0]},"; tok=("${tok[@]:1}"); done
      env "${envs[@]}" TAG="${tag%,}" /home/vt/perf/P2/bench.sh "${tok[0]}" "${tok[1]}" "${tok[2]}" "$TIME" "r$r-$name" "${tok[@]:3}"
    done <"$MATRIX"
  done
done

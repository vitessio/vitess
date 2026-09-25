#!/bin/bash
# fresh.sh: recreate the P5 cluster (env passes through: BIN DURABILITY VTGATE_EXTRA_FLAGS VTTABLET_EXTRA_FLAGS TABLE_SIZE ...)
S=/tmp/claude-0/-home-user-vitess/d4e874e9-36db-544e-abf5-6cb4fd4871ca/scratchpad/P5
$S/vtorc.sh stop >/dev/null 2>&1
/home/vt/perf/P5/c.sh down >/dev/null 2>&1
/home/vt/perf/P5/c.sh up >/dev/null 2>&1 || { echo "up failed"; exit 1; }
[[ -n $NOPREP ]] || /home/vt/perf/P5/c.sh prepare >/dev/null 2>&1
/home/vt/bin/vtctldclient --server localhost:40021 GetTablets | awk '{print $1, $4}' | tr '\n' ' '; echo

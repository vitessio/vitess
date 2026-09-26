#!/bin/bash
# suite.sh LABEL [FILTER]: run the P7 workloads once, one line each (bench.sh format).
LABEL=$1; FILTER=${2:-.}
B=/home/vt/perf/P7/bench.sh
T=${TIME:-20}
TS=${TIME_SCATTER:-15}
while read -r name thr time test args; do
  [[ -z $name || $name == \#* ]] && continue
  [[ $name =~ $FILTER ]] || continue
  [[ $time == T ]] && time=$T
  [[ $time == TS ]] && time=$TS
  # shellcheck disable=SC2086
  $B "$LABEL" "$name" "$thr" "$time" "$test" $args
done <<'EOF'
ps8           8  T  oltp_point_select --db-ps-mode=auto
ps32          32 T  oltp_point_select --db-ps-mode=auto
text8         8  T  oltp_point_select --db-ps-mode=disable
text32        32 T  oltp_point_select --db-ps-mode=disable
read_only     8  T  oltp_read_only
write_only    8  T  oltp_write_only
read_write    8  T  oltp_read_write
update_index  8  T  oltp_update_index
join_x10      8  TS p3:join_x --width=10
grp_high10k   8  TS p3:grp_high --width=10000
big10k_olap   8  TS p3:big --width=10000 --olap=on
EOF

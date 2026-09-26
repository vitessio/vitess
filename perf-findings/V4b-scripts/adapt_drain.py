#!/usr/bin/env python3
# One-off: adapt V4's drain.sh / abdrain.sh (copied here) to the V4b cluster (dst2 only, 200k-row tables).
p = '/home/vt/perf/V4b/drain.sh'
s = open(p).read()
s = s.replace('D=/home/vt/perf/V4\n', 'D=/home/vt/perf/V4b\n')
s = s.replace('uids() { case $1 in dst2) echo "101 102" ;; dst4) echo "103 104 105 106" ;; esac; }',
              'uids() { case $1 in dst2) echo "101 102" ;; esac; }')
s = s.replace('$VC Workflow --keyspace dst4 start --workflow wf_dst4 >/dev/null 2>&1\n', '')
s = s.replace('" "dst2 dst4"\n', '" "dst2"\n')
s = s.replace('$VC Workflow --keyspace dst4 stop --workflow wf_dst4 >/dev/null\n', '')
s = s.replace('--table-size=500000', '--table-size=200000')
s = s.replace('"http://localhost:40103/debug', '"http://localhost:40101/debug')
s = s.replace("awk -v r=\"$EVENTS\" '{d=$4-$2;", "awk -v r=\"$EVENTS\" '$1 ~ /_10[012]$/ {d=$4-$2;")
open(p, 'w').write(s)
p = '/home/vt/perf/V4b/abdrain.sh'
s = open(p).read()
s = s.replace('D=/home/vt/perf/V4\n', 'D=/home/vt/perf/V4b\n')
s = s.replace('NS=${NS:-"dst2 dst4 dst2+dst4"}', 'NS=${NS:-"dst2"}')
s = s.replace('BIN=$bin VTTABLET_EXTRA_FLAGS="$flags" "$D/c.sh" restart-tablets >/dev/null 2>&1\n    sleep 15',
              'VTTABLET_EXTRA_FLAGS="$flags" "$D/setarm.sh" "$bin" >/dev/null 2>&1')
s = s.replace('/vttablet_10[1-6]/', '/vttablet_10[12]/').replace('/mysqld_10[1-6]/', '/mysqld_10[12]/')
open(p, 'w').write(s)

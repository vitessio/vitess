#!/bin/bash
# Cancel workflows mv01..mvW (keyspace mdst).  Usage: cancelwf.sh W
for w in $(seq 1 "$1"); do
  /home/vt/perf/V4b/c.sh vtctldclient MoveTables --workflow "$(printf 'mv%02d' "$w")" --target-keyspace mdst cancel >/dev/null 2>&1 || echo "cancel $w failed"
done
mysql -N -S /home/vt/perf/c40000/vt_0000000104/mysql.sock -u vt_dba -e "select count(*) from _vt.vreplication"

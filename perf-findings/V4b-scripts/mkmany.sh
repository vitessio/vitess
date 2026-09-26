#!/bin/bash
# Create T small tables with R rows each in keyspace many (tablet uid $SUID, default 107), directly through mysqld.
# Usage: mkmany.sh T R
T=$1
R=${2:-100}
SUID=${SUID:-103}
SOCK=/home/vt/perf/c40000/vt_0000000$SUID/mysql.sock
f=$(mktemp)
{
  echo "SET sql_log_bin=0;"
  for i in $(seq 1 "$T"); do
    n=$(printf 't%04d' "$i")
    echo "DROP TABLE IF EXISTS $n; CREATE TABLE $n (id BIGINT NOT NULL, k INT NOT NULL, c VARCHAR(64) NOT NULL, ts DATETIME NOT NULL, PRIMARY KEY (id), KEY (k)) ENGINE=InnoDB;"
    echo "INSERT INTO $n SELECT seq, seq*7 % 1000, CONCAT('row-', seq), '2026-01-01 00:00:00' FROM (WITH RECURSIVE s(seq) AS (SELECT 1 UNION ALL SELECT seq+1 FROM s WHERE seq < $R) SELECT seq FROM s) x;"
  done
} >"$f"
mysql -S "$SOCK" -u vt_dba vt_many <"$f"
rm -f "$f"
mysql -N -S "$SOCK" -u vt_dba -e "select count(*) from information_schema.tables where table_schema='vt_many'"

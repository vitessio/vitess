#!/bin/bash
# Drop tables t(FROM)..t(TO) of keyspace many directly on its mysqld (no binlog).  Usage: dropmany.sh FROM TO
SOCK=/home/vt/perf/c40000/vt_0000000103/mysql.sock
f=$(mktemp)
{
  echo "SET sql_log_bin=0;"
  for i in $(seq "$1" "$2"); do printf 'DROP TABLE IF EXISTS t%04d;\n' "$i"; done
} >"$f"
mysql -S "$SOCK" -u vt_dba vt_many <"$f"
rm -f "$f"
mysql -N -S "$SOCK" -u vt_dba -e "select count(*) from information_schema.tables where table_schema='vt_many'"

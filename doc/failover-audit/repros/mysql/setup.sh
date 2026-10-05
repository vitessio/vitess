#!/bin/bash
# Prepares the MySQL-only relay log experiments (no Vitess involved).
#
#   doc/failover-audit/repros/mysql/setup.sh            # as root; needs /usr/sbin/mysqld (MySQL 8.0)
#
# It copies these scripts into $RELAYLOG_DIR (default $HOME/relaylog-work), initializes three
# mysqld instances there (P on port 45001, R1 on 45002, R2 on 45003, all on 127.0.0.1) with the
# Vitess-like settings in mkcnf.sh, creates the replication user without writing to the binary
# log, and saves clean datadir snapshots (clean_<name>.tgz) that every experiment restores from.
# Run an experiment afterwards from anywhere, e.g.:
#
#   MODE=sqlstop N=3 bash $RELAYLOG_DIR/e1.sh graceful                       # relay log discarded
#   MODE=sqlstop N=3 bash $RELAYLOG_DIR/e1.sh graceful relay_log_recovery=0  # kept and applied
#   cd $RELAYLOG_DIR && ./verify_receiver_only_change.sh V1
#
# The experiments start mysqld as root (--user=root) and kill only the processes they started.
set -euo pipefail

SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
B="${RELAYLOG_DIR:-$HOME/relaylog-work}"
mkdir -p "$B"
cp "$SRC"/*.sh "$B"/
chmod +x "$B"/*.sh
cd "$B"

i=0
for n in P R1 R2; do
  port=$((45001 + i)); sid=$((101 + i)); i=$((i + 1))
  if [ -e "$B/$n/mysqld.pid" ]; then kill -9 "$(cat "$B/$n/mysqld.pid")" 2>/dev/null || true; fi
  rm -rf "${B:?}/$n" "clean_$n.tgz"
  ./mkcnf.sh "$n" "$port" "$sid"
  ./ctl.sh init "$n"
  ./ctl.sh start "$n"
  mysql -uroot -S "$B/$n/mysql.sock" -e "
    SET GLOBAL super_read_only = OFF;
    SET SESSION sql_log_bin = 0;
    CREATE USER 'vt_repl'@'%' IDENTIFIED BY 'replpw';
    GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'vt_repl'@'%';
    RESET MASTER;"
  ./ctl.sh stop "$n"
  tar czf "clean_$n.tgz" "$n/data" "$n/logs"
  echo "prepared $n (port $port, server_id $sid)"
done
echo "ready: scripts and snapshots are in $B"

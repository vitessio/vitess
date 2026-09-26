#!/bin/bash
# usage: mkcnf.sh name port serverid [extra lines...]
B=/tmp/claude-0/-home-user-vitess/c1d2d1ed-3e94-5708-8646-dafcf8718844/scratchpad/relaylog
n=$1; port=$2; sid=$3; shift 3
d=$B/$n
mkdir -p $d/data $d/tmp $d/logs
cat > $d/my.cnf <<EOC
[mysqld]
user = root
datadir = $d/data
innodb_data_home_dir = $d/data
innodb_log_group_home_dir = $d/data
log-error = $d/logs/error.log
log-bin = $d/logs/binlog
relay-log = $d/logs/relay-bin
relay-log-index = $d/logs/relay-bin.index
pid-file = $d/mysqld.pid
port = $port
server-id = $sid
socket = $d/mysql.sock
tmpdir = $d/tmp
secure-file-priv = $d/tmp
slow-query-log-file = $d/logs/slow.log
long_query_time = 2
slow-query-log
skip-name-resolve
connect_timeout = 30
innodb_lock_wait_timeout = 20
max_allowed_packet = 64M
max_connections = 500
# mysql8026.cnf
skip_replica_start
gtid_mode = ON
enforce_gtid_consistency
relay_log_recovery = 1
binlog_expire_logs_seconds = 259200
mysqlx = 0
plugin-load = rpl_semi_sync_source=semisync_source.so;rpl_semi_sync_replica=semisync_replica.so
loose_rpl_semi_sync_source_timeout = 1000000000000000000
loose_rpl_semi_sync_source_wait_no_replica = 1
super-read-only
replica_net_timeout = 8
bind-address = 127.0.0.1
innodb_buffer_pool_size = 64M
EOC
for l in "$@"; do echo "$l" >> $d/my.cnf; done

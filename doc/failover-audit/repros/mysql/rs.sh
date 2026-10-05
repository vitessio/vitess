#!/bin/bash
# brief replica status
B="${RELAYLOG_DIR:-$HOME/relaylog-work}"
n=$1; d=$B/$n
mysql -uroot -S $d/mysql.sock -e "SHOW REPLICA STATUS\G" | egrep "Replica_IO_Running:|Replica_SQL_Running:|Retrieved_Gtid_Set|Executed_Gtid_Set|Last_IO_Error:|Last_SQL_Error:|Relay_Log_File|Relay_Log_Pos|Replica_SQL_Running_State|Source_Port|Auto_Position|Relay_Source_Log_File|Exec_Source_Log_Pos"
echo "gtid_executed: $(mysql -uroot -S $d/mysql.sock -N -e 'select @@gtid_executed')"
echo "relay files: $(ls -l $d/logs/ | grep relay-bin | awk '{print $9"("$5")"}' | tr '\n' ' ')"

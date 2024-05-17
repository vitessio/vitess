#!/bin/bash

# Copyright 2019 The Vitess Authors.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This is a helper script to sync replicas for mysql.
# It handles the special case where the primary has purged bin logs that the replica requires.
# To use it place a mysql dump of the database on the same directory as this script.
# The name of the dump must be $KEYSPACE.sql. The script can also download the mysqldump for you.
# Replication is fixed by restoring the mysqldump and resetting replication.
# https://dev.mysql.com/doc/refman/5.7/en/replication-mode-change-online-disable-gtids.html
# https://www.percona.com/blog/2013/02/08/how-to-createrestore-a-slave-using-gtid-replication-in-mysql-5-6/

cd "$(dirname "${BASH_SOURCE[0]}")"

function get_replication_status() {
    # Get replication status
    STATUS_LINE=$(mysql -u$DB_USER -p$DB_PASS -h 127.0.0.1 -e "SHOW REPLICA STATUS\G")
    LAST_ERRNO=$(grep "Last_IO_Errno:" <<< "$STATUS_LINE" | awk '{ print $2 }')
    REPLICA_SQL_RUNNING=$(grep "Replica_SQL_Running:" <<< "$STATUS_LINE" | awk '{ print $2 }')
    REPLICA_IO_RUNNING=$(grep "Replica_IO_Running:" <<< "$STATUS_LINE" | awk '{ print $2 }')
    SOURCE_HOST=$(grep "Source_Host:" <<< "$STATUS_LINE" | awk '{ print $2 }')
    SOURCE_PORT=$(grep "Source_Port:" <<< "$STATUS_LINE" | awk '{ print $2 }')

    echo "Replica_SQL_Running: $REPLICA_SQL_RUNNING"
    echo "Replica_IO_Running: $REPLICA_IO_RUNNING"
    echo "Last_IO_Errno: $LAST_ERRNO"
}

function reset_replication() {
    # Necessary before sql file can be imported
    echo "Importing MysqlDump: $KEYSPACE.sql"
    mysql -u$DB_USER -p$DB_PASS -h 127.0.0.1 -e "RESET MASTER;STOP REPLICA;CHANGE REPLICATION SOURCE TO SOURCE_AUTO_POSITION = 0;source $KEYSPACE.sql;START REPLICA;"
    # Restore Master Auto Position
    echo "Restoring Master Auto Setting"
    mysql -u$DB_USER -p$DB_PASS -h 127.0.0.1 -e "STOP REPLICA;CHANGE REPLICATION SOURCE TO SOURCE_AUTO_POSITION = 1;START REPLICA;"
}

# Retrieve replication status
get_replication_status

# Exit script if called with argument 'status'
[ ${1:-''} != 'status' ] || exit 0;

# Check if IO_Thread is running
if [[ $REPLICA_IO_RUNNING = "No" && $LAST_ERRNO = 1236 ]]; then
    
    echo "Primary has purged bin logs that replica requires. Sync will require restore from mysqldump"
    if [[ -f $KEYSPACE.sql ]] ; then
        echo "mysqldump file $KEYSPACE.sql exists, attempting to restore.."
        echo "Resetting replication.."
        reset_replication
    else
        echo "Starting mysqldump. This may take a while.."
        # Modify flags to user's requirements
        if mysqldump -h $SOURCE_HOST -P $SOURCE_PORT -u$DB_USER -p$DB_PASS --databases $KEYSPACE \
            --triggers --routines --events --hex-blob  --master-data=1 --quick --order-by-primary \
            --no-autocommit --skip-comments --skip-add-drop-table --skip-add-locks \
            --skip-disable-keys --single-transaction --set-gtid-purged=on --verbose > $KEYSPACE.sql; then
            echo "mysqldump complete for database $KEYSPACE"
            echo "Resetting replication.."
            reset_replication
        else
            echo "mysqldump failed for database $KEYSPACE"
        fi
    fi

else

    echo "No Actions to perform"

fi

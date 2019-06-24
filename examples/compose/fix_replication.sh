#!/bin/bash

# This is a helper script to sync replicas for mysql.
# It handles the special case where the master has purged bin logs that the replica requires.
# To use it place a mysql dump of the database on the same directory as this script.
# The name of the dump must be $KEYSPACE.sql. The script can also download the mysqldump for you.
# Replication is fixed by restoring the mysqldump and resetting the slave.
# https://dev.mysql.com/doc/refman/5.7/en/replication-mode-change-online-disable-gtids.html
# https://www.percona.com/blog/2013/02/08/how-to-createrestore-a-slave-using-gtid-replication-in-mysql-5-6/

cd "$(dirname "${BASH_SOURCE[0]}")"

function get_slave_status() {
    # Get slave status
    STATUS_LINE=$(mysql -u$DB_USER -p$DB_PASS -h 127.0.0.1 -e "SHOW SLAVE STATUS\G")
    LAST_ERRNO=$(grep "Last_IO_Errno:" <<< "$STATUS_LINE" | awk '{ print $2 }')
    SLAVE_SQL_RUNNING=$(grep "Slave_SQL_Running:" <<< "$STATUS_LINE" | awk '{ print $2 }')
    SLAVE_IO_RUNNING=$(grep "Slave_IO_Running:" <<< "$STATUS_LINE" | awk '{ print $2 }')
    MASTER_HOST=$(grep "Master_Host:" <<< "$STATUS_LINE" | awk '{ print $2 }')
    MASTER_PORT=$(grep "Master_Port:" <<< "$STATUS_LINE" | awk '{ print $2 }')

    echo "Slave_SQL_Running: $SLAVE_SQL_RUNNING"
    echo "Slave_IO_Running: $SLAVE_IO_RUNNING"
    echo "Last_IO_Errno: $LAST_ERRNO"
}

function reset_slave() {
    # Necessary before sql file can be imported
    echo "Importing MysqlDump: $KEYSPACE.sql"
    mysql -u$DB_USER -p$DB_PASS -h 127.0.0.1 -e "RESET MASTER;STOP SLAVE;CHANGE MASTER TO MASTER_AUTO_POSITION = 0;source $KEYSPACE.sql;START SLAVE;"
    # Restore Master Auto Position
    echo "Restoring Master Auto Setting"
    mysql -u$DB_USER -p$DB_PASS -h 127.0.0.1 -e "STOP SLAVE;CHANGE MASTER TO MASTER_AUTO_POSITION = 1;START SLAVE;"
}

# Retrieve slave status
get_slave_status

# Exit script if called with argument 'status'
[ ${1:-''} != 'status' ] || exit 0;

# Check if SLAVE_IO is running
if [[ $SLAVE_IO_RUNNING = "No" && $LAST_ERRNO = 1236 ]]; then
    
    echo "Master has purged bin logs that slave requires. Sync will require restore from mysqldump"
    if [[ -f $KEYSPACE.sql ]] ; then
        echo "mysqldump file $KEYSPACE.sql exists, attempting to restore.."
        echo "Resetting slave.."
        reset_slave
    else
        echo "Starting mysqldump. This may take a while.."
        # Modify flags to user's requirements
        if mysqldump -h $MASTER_HOST -P $MASTER_PORT -u$DB_USER -p$DB_PASS --databases $KEYSPACE \
            --triggers --routines --events --hex-blob  --master-data=1 --quick --order-by-primary \
            --no-autocommit --skip-comments --skip-add-drop-table --skip-add-locks \
            --skip-disable-keys --single-transaction --set-gtid-purged=on --verbose > $KEYSPACE.sql ]]; then
            echo "mysqldump complete for database $KEYSPACE"
            echo "Resetting slave.."
            reset_slave
        else
            echo "mysqldump failed for database $KEYSPACE"
        fi
    fi

else

    echo "No Actions to perform"

fi

#!/bin/bash

# Copyright 2025 The Vitess Authors.
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

# This is an example script that uses MySQL on 3306 as a topo server.
source "$(dirname "${BASH_SOURCE[0]:-$0}")/../env.sh"

cell=${CELL:-'test'}

echo "Setting up mysql topo.."

# Check if MySQL is already running
# Extract user, password, host and port from MYSQL_TOPO_ADDR (format: user:password@tcp(host:port)/database)
MYSQL_USER_PASS=$(echo "$MYSQL_TOPO_ADDR" | sed 's/@.*//')
MYSQL_USER=$(echo "$MYSQL_USER_PASS" | cut -d: -f1)
MYSQL_PASS=$(echo "$MYSQL_USER_PASS" | cut -d: -f2)
MYSQL_HOST_PORT=$(echo "$MYSQL_TOPO_ADDR" | sed 's/.*@tcp(\([^)]*\)).*/\1/')
MYSQL_HOST=$(echo "$MYSQL_HOST_PORT" | cut -d: -f1)
MYSQL_PORT=$(echo "$MYSQL_HOST_PORT" | cut -d: -f2)
CREATEUSER="CREATE USER IF NOT EXISTS '${MYSQL_USER}'@'%' IDENTIFIED WITH mysql_native_password BY '${MYSQL_PASS}';"
GRANTTOPO="GRANT ALL PRIVILEGES ON topo.* TO '${MYSQL_USER}'@'%'; GRANT REPLICATION SLAVE ON *.* TO '${MYSQL_USER}'@'%';"

# For the MySQL topo implementation we expect to have a MySQL server running.
# We first try to connect as the extracted user with the extracted password.
if mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASS" -e "SELECT 1" > /dev/null 2>&1; then
    echo "MySQL is already running on $MYSQL_HOST:$MYSQL_PORT"
elif mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u root -e "SELECT 1" > /dev/null 2>&1; then
    # if that fails, we try to connect as root with no password *and* create the topo user and then execute the GRANT statement.
    # if that fails, we bail and print an error message saying please create the topo user with this CREATE USER + GRANT statement.
    echo "MySQL is running, but '${MYSQL_USER}' does not exist. Attempting to create '${MYSQL_USER}' and grant privileges..."
    if ! mysql -u "$MYSQL_USER" -p"$MYSQL_PASS" -e "SELECT 1" 2>/dev/null; then
        if ! mysql -u root -e "$CREATEUSER" 2>/dev/null; then
            echo "Failed to create user '${MYSQL_USER}'@'%'."
            echo "Please create the user manually with the following commands:"
            echo "  mysql -u root -e \"$CREATEUSER\""
            echo "  mysql -u root -e \"$GRANTTOPO\""
            exit 1
        fi
        if ! mysql -u root -e "$GRANTTOPO" 2>/dev/null; then
            echo "Failed to grant privileges to '${MYSQL_USER}'@'%'."
            echo "Please run the following command manually:"
            echo "  mysql -u root -e \"$GRANTTOPO\""
            exit 1
        fi
    fi

else
    echo "MySQL is not running, attempting to start it..."
    
    # Check if we're running in Docker (by checking for /.dockerenv file)
    if [[ -f /.dockerenv ]]; then
        echo "Detected Docker environment, starting MySQL..."
        
        # Create MySQL data directory and tmp directory
        mkdir -p "${VTDATAROOT}/mysql-topo"
        mkdir -p "${VTDATAROOT}/tmp"
        
        # Initialize MySQL data directory if it doesn't exist
        if [[ ! -d "${VTDATAROOT}/mysql-topo/mysql" ]]; then
            echo "Initializing MySQL data directory..."
            mysqld --initialize-insecure --user=vitess --datadir="${VTDATAROOT}/mysql-topo" > "${VTDATAROOT}/tmp/mysql-init.out" 2>&1
        fi
        
        # Start MySQL server
        echo "Starting MySQL server..."
        mysqld --user=vitess \
               --datadir="${VTDATAROOT}/mysql-topo" \
               --socket="${VTDATAROOT}/mysql-topo/mysql.sock" \
               --pid-file="${VTDATAROOT}/mysql-topo/mysql.pid" \
               --port="$MYSQL_PORT" \
               --bind-address="$MYSQL_HOST" \
               --skip-networking=false \
               --skip-mysqlx \
               --gtid-mode=ON \
               --enforce-gtid-consistency \
               --log-bin \
               --log-error="${VTDATAROOT}/tmp/mysql-topo.err" \
               > "${VTDATAROOT}/tmp/mysql-topo.out" 2>&1 &
        
        PID=$!
        echo $PID > "${VTDATAROOT}/tmp/mysql-topo.pid"
        
        # Wait for MySQL to start
        echo "Waiting for MySQL to start..."
        for i in {1..30}; do
            if mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u root -e "SELECT 1" > /dev/null 2>&1; then
                echo "MySQL started successfully"
                break
            fi
            if [[ $i -eq 30 ]]; then
                echo "Failed to start MySQL after 30 seconds"
                echo "MySQL error log:"
                cat "${VTDATAROOT}/tmp/mysql-topo.err" 2>/dev/null || echo "No error log found"
                exit 1
            fi
            sleep 1
        done
        
        # Create the topo database
        echo "Creating topo database..."
        mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u root -e "CREATE DATABASE IF NOT EXISTS topo;" || {
            echo "Failed to create topo database"
            exit 1
        }
        # Creating the topo user and granting privileges
        echo "Creating topo user and granting privileges..."
        mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u root -e "$CREATEUSER" || {
            echo "Failed to create user '${MYSQL_USER}'@'%'"
            exit 1
        }
        mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u root -e "$GRANTTOPO" || {
            echo "Failed to grant privileges to '${MYSQL_USER}'@'%'"
            exit 1
        }
        
    else
        echo "Error: MySQL is not running and this script can only auto-start MySQL in Docker environments."
        echo "Please start MySQL manually before calling mysql-up.sh"
        echo "You can start MySQL with a command like:"
        echo "  sudo systemctl start mysql"
        echo "  # or"
        echo "  sudo service mysql start"
        echo "  # or start it manually with mysqld"
        exit 1
    fi
fi

mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASS" -e "CREATE DATABASE IF NOT EXISTS topo" || true

# And also add the CellInfo description for the cell.
# If the node already exists, it's fine, means we used existing data.
echo "add ${cell} CellInfo"
set +e
command vtctldclient --server internal --topo-implementation mysql --topo-global-server-address $MYSQL_TOPO_ADDR AddCellInfo \
  --root "/vitess/${cell}" \
  --server-address "$MYSQL_TOPO_ADDR" \
  "${cell}"
set -e

echo "mysql topo set!"

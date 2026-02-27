#!/bin/bash

set -euo pipefail

echo '**********GRANTING PRIVILEGES START*******************'
# PURGE BINARY LOGS BEFORE DATE(NOW());
mysql --protocol=socket -uroot -hlocalhost --socket=/var/run/mysqld/mysqld.sock -p"$MYSQL_ROOT_PASSWORD" -e \
"GRANT ALL PRIVILEGES ON *.* TO '$MYSQL_USER'@'%' IDENTIFIED BY '$MYSQL_PASSWORD'"
echo '*************GRANTING PRIVILEGES END****************'

#!/bin/bash

# This script will generate a new bootstrap archive for the currently installed
# flavor and version of MySQL by running mysql_upgrade.

if [ -z "$VTDATAROOT" ]; then
  echo "You must source dev.env before running this."
  exit 1
fi

mysql_port=33306
tablet_uid=99999
tablet_dir=$VTDATAROOT/vt_0000099999

set -e

echo Starting mysqld
mysqlctl -tablet_uid=$tablet_uid -mysql_port=$mysql_port init -skip_schema

echo Running mysql_upgrade
mysql_upgrade --socket=$tablet_dir/mysql.sock --user=vt_dba 

echo Stopping mysqld
mysqlctl -tablet_uid=$tablet_uid -mysql_port=$mysql_port shutdown

newfile=mysql-db-dir_$(cat $tablet_dir/data/mysql_upgrade_info).tbz

echo Creating new bootstrap file: $newfile
(cd $tablet_dir && tar -jcf data.tbz data innodb)
mv $tablet_dir/data.tbz ./$newfile

echo Removing tablet directory
rm -r $tablet_dir
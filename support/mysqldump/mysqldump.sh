#!/bin/bash

# Define your variables
SOURCE_USER=root
SOURCE_HOST=localhost
SOURCE_PASSWORD=password
SOURCE_DATABASE=source_dbname

TARGET_USER=vtgate-test-user
TARGET_HOST=vtgate-host
TARGET_PASSWORD=vtgate-password
TARGET_DATABASE=destination_keyspace

SECURE='--ssl-mode=PREFER'

# Uncomment if you have a ca certificate.
# SECURE='--ssl-ca=./vtgate-ca-cert.pem --ssl-mode=VERIFY_CA'

# Dump and restore schema only
mysqldump -d $SOURCE_DATABASE --user=$SOURCE_USER --host=$SOURCE_HOST --password=$SOURCE_PASSWORD \
--single-transaction --verbose | \
sed 's/^\(\(LOCK\|UNLOCK\|CHANGE\|SET\).*\)$/-- m \1/ ; /.*CONSTRAINT.*/d ; s/^\(\/\*![0-9]\{5\}.*\(TIME_ZONE\|SQL_NOTES\|CHARACTER_SET\).*\/;\)$/-- m \1/I ; $!N;s/^\(\s*[^C].*\),\(\n\s*CONSTRAINT.*\)$/\1\2/;P;D' > ./schema.sql
mysql --user=$TARGET_USER --host=$TARGET_HOST --password=$TARGET_PASSWORD $TARGET_DATABASE $SECURE < ./schema.sql

# Dump and Restore Data Only
mysqldump $SOURCE_DATABASE --user=$SOURCE_USER --host=$SOURCE_HOST --password=$SOURCE_PASSWORD \
--no-create-info --triggers --routines --events --hex-blob --quick --order-by-primary \
--no-autocommit --skip-comments --quote-names --no-tablespaces --skip-add-locks \
--skip-disable-keys --single-transaction --set-gtid-purged=off --verbose --lock-tables=off | \
sed 's/^\(\(LOCK\|UNLOCK\|CHANGE\|SET\).*\)$/-- m \1/ ; s/^\(\/\*![0-9]\{5\}.*\(TIME_ZONE\|SQL_NOTES\|CHARACTER_SET\).*\/;\)$/-- m \1/I ; s/'\''0000-00-00 00:00:00'\''/CURRENT_TIMESTAMP/g' > ./data.sql
mysql --user=$TARGET_USER --host=$TARGET_HOST --password=$TARGET_PASSWORD $TARGET_DATABASE $SECURE < ./data.sql

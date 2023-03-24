# This file is executed immediately after mysql_install_db,
# to initialize a fresh data directory.
###############################################################################
# Equivalent of mysql_secure_installation
###############################################################################
# We need to ensure that super_read_only is disabled so that we can execute
# these commands. Note that disabling it does NOT disable read_only.
# We save the current value so that we only re-enable it at the end if it was
# enabled before.
SET @original_super_read_only=IF(@@global.super_read_only=1, 'ON', 'OFF');
SET GLOBAL super_read_only='OFF';
# Changes during the init db should not make it to the binlog.
# They could potentially create errant transactions on replicas.
SET sql_log_bin = 0;
# Remove anonymous users.
DELETE FROM mysql.user WHERE User = '';
# Disable remote root access (only allow UNIX socket).
DELETE FROM mysql.user WHERE User = 'root' AND Host != 'localhost';
# Remove test database.
DROP DATABASE IF EXISTS test;
###############################################################################
# Vitess defaults
###############################################################################
# Vitess-internal database.
CREATE DATABASE IF NOT EXISTS _vt;
# Note that definitions of local_metadata and shard_metadata should be the same
# as in production which is defined in go/vt/mysqlctl/metadata_tables.go.
CREATE TABLE IF NOT EXISTS _vt.local_metadata (
  name VARCHAR(255) NOT NULL,
  value VARCHAR(255) NOT NULL,
  db_name VARBINARY(255) NOT NULL,
  PRIMARY KEY (db_name, name)
  ) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS _vt.shard_metadata (
  name VARCHAR(255) NOT NULL,
  value MEDIUMBLOB NOT NULL,
  db_name VARBINARY(255) NOT NULL,
  PRIMARY KEY (db_name, name)
  ) ENGINE=InnoDB;
# Admin user with all privileges.
CREATE USER 'vt_dba'@'localhost';
GRANT ALL ON *.* TO 'vt_dba'@'localhost';
GRANT GRANT OPTION ON *.* TO 'vt_dba'@'localhost';
# User for app traffic, with global read-write access.
CREATE USER 'vt_app'@'localhost';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, FILE,
  REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES,
  LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW,
  SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER
  ON *.* TO 'vt_app'@'localhost';
# User for app debug traffic, with global read access.
CREATE USER 'vt_appdebug'@'localhost';
GRANT SELECT, SHOW DATABASES, PROCESS ON *.* TO 'vt_appdebug'@'localhost';
# User for administrative operations that need to be executed as non-SUPER.
# Same permissions as vt_app here.
CREATE USER 'vt_allprivs'@'localhost';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, FILE,
  REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES,
  LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW,
  SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER
  ON *.* TO 'vt_allprivs'@'localhost';
# User for slave replication connections.
# TODO: Should we set a password on this since it allows remote connections?
CREATE USER 'vt_repl'@'%';
GRANT REPLICATION SLAVE ON *.* TO 'vt_repl'@'%';
# User for Vitess filtered replication (binlog player).
# Same permissions as vt_app.
CREATE USER 'vt_filtered'@'localhost';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, FILE,
  REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES,
  LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW,
  SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER
  ON *.* TO 'vt_filtered'@'localhost';
FLUSH PRIVILEGES;
RESET SLAVE ALL;
RESET MASTER;
# custom sql is used to add custom scripts like creating users/passwords. We use it in our tests
# {{custom_sql}}

# We need to set super_read_only back to what it was before
SET GLOBAL super_read_only=IFNULL(@original_super_read_only, 'ON');

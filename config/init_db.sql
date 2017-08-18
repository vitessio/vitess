# This file is executed immediately after mysql_install_db,
# to initialize a fresh data directory.

##########################################
# Equivalent of mysql_secure_installation
##########################################

# Remove anonymous users.
DELETE FROM mysql.user WHERE User = '';

# Disable remote root access (only allow UNIX socket).
DELETE FROM mysql.user WHERE User = 'root' AND Host != 'localhost';

# Remove test database.
DROP DATABASE IF EXISTS test;

##########################################
# Vitess defaults
##########################################

# Vitess-internal database.
CREATE DATABASE IF NOT EXISTS _vt;
# Note that definitions of local_metadata and shard_metadata should be the same
# as in production which is defined in go/vt/mysqlctl/metadata_tables.go.
CREATE TABLE IF NOT EXISTS _vt.local_metadata (
  name VARCHAR(255) NOT NULL,
  value VARCHAR(255) NOT NULL,
  PRIMARY KEY (name)
  ) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS _vt.shard_metadata (
  name VARCHAR(255) NOT NULL,
  value MEDIUMBLOB NOT NULL,
  PRIMARY KEY (name)
  ) ENGINE=InnoDB;

# Admin user with all privileges.
GRANT ALL ON *.* TO 'vt_dba'@'localhost';
GRANT GRANT OPTION ON *.* TO 'vt_dba'@'localhost';

# User for app traffic, with global read-write access.
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, FILE,
  REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES,
  LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW,
  SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER
  ON *.* TO 'vt_app'@'localhost';

# User for app debug traffic, with global read access.
GRANT SELECT, SHOW DATABASES, PROCESS ON *.* TO 'vt_appdebug'@'localhost';

# User for administrative operations that need to be executed as non-SUPER.
# Same permissions as vt_app here.
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, FILE,
  REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES,
  LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW,
  SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER
  ON *.* TO 'vt_allprivs'@'localhost';

# User for slave replication connections.
GRANT REPLICATION SLAVE ON *.* TO 'vt_repl'@'%';

# User for Vitess filtered replication (binlog player).
# Same permissions as vt_app.
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, FILE,
  REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES,
  LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW,
  SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER
  ON *.* TO 'vt_filtered'@'localhost';

# User for Orchestrator (https://github.com/github/orchestrator).
GRANT SUPER, PROCESS, REPLICATION SLAVE, RELOAD
  ON *.* TO 'orc_client_user'@'%' IDENTIFIED BY 'orc_client_user_password';
GRANT SELECT
  ON _vt.* TO 'orc_client_user'@'%' IDENTIFIED BY 'orc_client_user_password';

FLUSH PRIVILEGES;


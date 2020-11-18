#SET sql_log_bin = 0;

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
CREATE USER 'vt_dba'@'%';
GRANT ALL ON _vt.* TO 'vt_dba'@'%';
GRANT ALL ON auroratest1.* TO 'vt_dba'@'%';
GRANT GRANT OPTION ON _vt.* TO 'vt_dba'@'%';
GRANT GRANT OPTION ON auroratest1.* TO 'vt_dba'@'%';

# User for app traffic, with global read-write access.
CREATE USER 'vt_app'@'%';
GRANT ALL ON _vt.* TO 'vt_app'@'%';
GRANT ALL ON auroratest1.* TO 'vt_app'@'%';


# User for app debug traffic, with global read access.
CREATE USER 'vt_appdebug'@'%';
GRANT SELECT, SHOW DATABASES, PROCESS ON *.* TO 'vt_appdebug'@'%';

# User for administrative operations that need to be executed as non-SUPER.
# Same permissions as vt_app here.
CREATE USER 'vt_allprivs'@'%';
GRANT ALL ON _vt.* TO 'vt_allprivs'@'%';
GRANT ALL ON auroratest1.* TO 'vt_allprivs'@'%';


# User for slave replication connections.
# ...NOT SURE THIS IS NEEDED OR EVEN WORKS...
CREATE USER 'vt_repl'@'%';
GRANT ALL ON auroratest1.* TO 'vt_repl'@'%';


# User for Vitess filtered replication (binlog player).
# Same permissions as vt_app.
CREATE USER 'vt_filtered'@'%';
GRANT ALL ON _vt.* TO 'vt_filtered'@'%';
GRANT ALL ON auroratest1.* TO 'vt_filtered'@'%';

# User for general MySQL monitoring.
CREATE USER 'vt_monitoring'@'%';
GRANT ALL ON _vt.* TO 'vt_monitoring'@'%';
GRANT ALL ON auroratest1.* TO 'vt_monitoring'@'%';
GRANT SELECT, UPDATE, DELETE, DROP
  ON performance_schema.* TO 'vt_monitoring'@'%';

# Skipping Orchestrator
# User for Orchestrator (https://github.com/github/orchestrator).
#CREATE USER 'orc_client_user'@'%' IDENTIFIED BY 'orc_client_user_password';
#GRANT SUPER, PROCESS, REPLICATION SLAVE, RELOAD
#  ON *.* TO 'orc_client_user'@'%';
#GRANT SELECT
#  ON _vt.* TO 'orc_client_user'@'%';

FLUSH PRIVILEGES;

RESET SLAVE ALL;
# RESET MASTER;


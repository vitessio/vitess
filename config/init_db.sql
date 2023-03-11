# This file is executed immediately after initializing a fresh data directory.

###############################################################################
# WARNING: This sql is *NOT* safe for production use,
#          as it contains default well-known users and passwords.
#          Care should be taken to change these users and passwords
#          for production.
###############################################################################

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

# Admin user with all privileges.
CREATE USER 'vt_dba'@'localhost';
GRANT ALL ON *.* TO 'vt_dba'@'localhost';
GRANT GRANT OPTION ON *.* TO 'vt_dba'@'localhost';

# User for app traffic, with global read-write access.
CREATE USER 'vt_app'@'localhost';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, FILE,
  REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES,
  LOCK TABLES, EXECUTE, REPLICATION CLIENT, CREATE VIEW,
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
CREATE USER 'vt_repl'@'%';
GRANT REPLICATION SLAVE ON *.* TO 'vt_repl'@'%';

# User for Vitess VReplication (base vstreamers and vplayer).
CREATE USER 'vt_filtered'@'localhost';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, FILE,
  REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES,
  LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW,
  SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER
  ON *.* TO 'vt_filtered'@'localhost';

# User for general MySQL monitoring.
CREATE USER 'vt_monitoring'@'localhost';
GRANT SELECT, PROCESS, SUPER, REPLICATION CLIENT, RELOAD
  ON *.* TO 'vt_monitoring'@'localhost';
GRANT SELECT, UPDATE, DELETE, DROP
  ON performance_schema.* TO 'vt_monitoring'@'localhost';

FLUSH PRIVILEGES;

RESET SLAVE ALL;
RESET MASTER;

# custom sql is used to add custom scripts like creating users/passwords. We use it in our tests
# {{custom_sql}}

# We need to set super_read_only back to what it was before
SET GLOBAL super_read_only=IFNULL(@original_super_read_only, 'ON');

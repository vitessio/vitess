# This file is executed immediately after mysql_install_db,
# to initialize a fresh data directory.

###############################################################################
# WARNING: This sql is *NOT* safe for production use,
#          as it contains default well-known users and passwords.
#          Care should be taken to change these users and passwords
#          for production.
###############################################################################

###############################################################################
# Equivalent of mysql_secure_installation
###############################################################################

# Remove anonymous users.
# DELETE FROM mysql.user WHERE User = '';

# Disable remote root access (only allow UNIX socket).
# DELETE FROM mysql.user WHERE User = 'root' AND Host != 'localhost';

# Remove test database.
DROP DATABASE IF EXISTS test;

###############################################################################
# Vitess defaults
###############################################################################

# Admin user with all privileges.
CREATE USER 'vt_dba'@'localhost';
GRANT ALL ON *.* TO 'vt_dba'@'localhost';
GRANT GRANT OPTION ON *.* TO 'vt_dba'@'localhost';

CREATE USER 'vt_dba'@'gateway';
GRANT ALL ON *.* TO 'vt_dba'@'gateway';
GRANT GRANT OPTION ON *.* TO 'vt_dba'@'gateway';

# User for app traffic, with global read-write access.
CREATE USER 'vt_app'@'localhost';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, FILE,
  REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES,
  LOCK TABLES, EXECUTE, REPLICATION CLIENT, CREATE VIEW,
  SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER
  ON *.* TO 'vt_app'@'localhost';

CREATE USER 'vt_app'@'gateway';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, FILE,
    REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES,
  LOCK TABLES, EXECUTE, REPLICATION CLIENT, CREATE VIEW,
    SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER
  ON *.* TO 'vt_app'@'gateway';

# User for app debug traffic, with global read access.
CREATE USER 'vt_appdebug'@'localhost';
GRANT SELECT, SHOW DATABASES, PROCESS ON *.* TO 'vt_appdebug'@'localhost';

CREATE USER 'vt_appdebug'@'gateway';
GRANT SELECT, SHOW DATABASES, PROCESS ON *.* TO 'vt_appdebug'@'gateway';

# User for administrative operations that need to be executed as non-SUPER.
# Same permissions as vt_app here.
CREATE USER 'vt_allprivs'@'localhost';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, FILE,
  REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES,
  LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW,
  SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER
  ON *.* TO 'vt_allprivs'@'localhost';

CREATE USER 'vt_allprivs'@'gateway';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, FILE,
    REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES,
  LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW,
    SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER
  ON *.* TO 'vt_allprivs'@'gateway';

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

CREATE USER 'vt_filtered'@'gateway';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, FILE,
    REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES,
  LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW,
    SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER
  ON *.* TO 'vt_filtered'@'gateway';


# User for general MySQL monitoring.
CREATE USER 'vt_monitoring'@'localhost';
GRANT SELECT, PROCESS, SUPER, REPLICATION CLIENT, RELOAD
  ON *.* TO 'vt_monitoring'@'localhost';
GRANT SELECT, UPDATE, DELETE, DROP
  ON performance_schema.* TO 'vt_monitoring'@'localhost';

CREATE USER 'vt_monitoring'@'gateway';
GRANT SELECT, PROCESS, SUPER, REPLICATION CLIENT, RELOAD
      ON *.* TO 'vt_monitoring'@'gateway';
GRANT SELECT, UPDATE, DELETE, DROP
      ON performance_schema.* TO 'vt_monitoring'@'gateway';

FLUSH PRIVILEGES;


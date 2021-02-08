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

# Changes during the init db should not make it to the binlog.
# They could potentially create errant transactions on replicas.
SET sql_log_bin = 0;
# Remove anonymous users.
#DELETE FROM mysql.user WHERE User = '';

# Disable remote root access (only allow UNIX socket).
#DELETE FROM mysql.user WHERE User = 'root' AND Host != 'localhost';
# Remove anonymous users and non-localhost root users.

# use standard DROP USER on anon users and the local root TCP based user
# The default standard install uses @@hostname as the hostname, but only
# MariaDB(10.2.3+) supports the EXECUTE IMMEDIATE.

DROP USER /*M!100103 IF EXISTS */ ''@localhost;
/*M!100203 EXECUTE IMMEDIATE CONCAT('DROP USER IF EXISTS ""@', @@hostname) */;

DROP USER /*M!100103 IF EXISTS */ 'root'@'127.0.0.1';
DROP USER /*M!100103 IF EXISTS */ 'root'@'::1';
/*M!100203 EXECUTE IMMEDIATE CONCAT('DROP USER IF EXISTS root@', @@hostname) */;

## MariaDB-10.3 can be a bit more thorough with this FOR synax.
#DELIMITER $$
#/*M!100301 CREATE OR REPLACE PROCEDURE mysql.secure_users()
#FOR insecuser IN ( SELECT user,host FROM mysql.user WHERE user='' OR (user='root' AND host!='localhost') )
#DO
#	EXECUTE IMMEDIATE CONCAT('DROP USER `', insecuser.user, '`@`', insecuser.host, '`');
#END FOR */
#$$
#DELIMITER ;
#/*M!100301 call mysql.secure_users() */;
#/*M!100301 DROP PROCEDURE mysql.secure_users */;


## MariaDB-10.4 can be a bit more thorough with this FOR synax.
DROP PROCEDURE IF EXISTS mysql.secure_install;

DELIMITER $$
CREATE PROCEDURE mysql.secure_install()
BEGIN
	DECLARE done INT DEFAULT FALSE;
	DECLARE u CHAR(80);
	DECLARE h CHAR(60);
	DECLARE deluser CURSOR FOR SELECT user,host FROM mysql.user WHERE user='' OR (user='root' AND host!='localhost');
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

	OPEN deluser;

	del_loop: LOOP
	FETCH deluser INTO u,h;
	IF done THEN
          LEAVE del_loop;
        END IF;
	SELECT CONCAT("DROP USER ",QUOTE(u), "@", QUOTE(h)) into @sqldropuser;
	PREPARE dodeluser FROM @sqldropuser;
	EXECUTE dodeluser;
        DEALLOCATE PREPARE dodeluser;
	END LOOP;

	CLOSE deluser;
END$$

DELIMITER ;
CALL mysql.secure_install();
DROP PROCEDURE mysql.secure_install;


# MySQL-5.7 (not MariaDB) can use direct table manipulation. MariaDB-10.4 has mysql.user as a VIEW
# so the previous cases of MariaDB will work.
/*!50701 DELETE FROM mysql.user WHERE User= '' OR (User = 'root' AND Host != 'localhost') */;
/*!50701 FLUSH PRIVILEGES */;

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

# User for general MySQL monitoring.
CREATE USER 'vt_monitoring'@'localhost';
GRANT SELECT, PROCESS, SUPER, REPLICATION CLIENT, RELOAD
  ON *.* TO 'vt_monitoring'@'localhost';
GRANT SELECT, UPDATE, DELETE, DROP
  ON performance_schema.* TO 'vt_monitoring'@'localhost';

# User for Orchestrator (https://github.com/openark/orchestrator).
CREATE USER 'orc_client_user'@'%' IDENTIFIED BY 'orc_client_user_password';
GRANT SUPER, PROCESS, REPLICATION SLAVE, RELOAD
  ON *.* TO 'orc_client_user'@'%';
GRANT SELECT
  ON _vt.* TO 'orc_client_user'@'%';

#FLUSH PRIVILEGES;

RESET SLAVE ALL;
RESET MASTER;

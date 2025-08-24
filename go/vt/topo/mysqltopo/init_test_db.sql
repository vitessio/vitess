-- MySQL initialization file for mysqltopo tests
-- This file creates a user with proper permissions for topo tests

-- Create topo user with mysql_native_password authentication
CREATE USER 'topo'@'%' IDENTIFIED WITH mysql_native_password BY 'topopass';

-- Grant all privileges needed for topo operations
GRANT ALL PRIVILEGES ON *.* TO 'topo'@'%';
GRANT REPLICATION SLAVE ON *.* TO 'topo'@'%';

-- Also recreate the vt_dba user with mysql_native_password
DROP USER IF EXISTS 'vt_dba'@'localhost';
CREATE USER 'vt_dba'@'localhost' IDENTIFIED WITH mysql_native_password;
GRANT ALL ON *.* TO 'vt_dba'@'localhost';
GRANT GRANT OPTION ON *.* TO 'vt_dba'@'localhost';

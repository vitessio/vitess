CREATE DATABASE _vt;
CREATE TABLE _vt.replication_test (time_created_ns bigint primary key);
CREATE TABLE _vt.replication_log (time_created_ns bigint primary key, note varchar(255));

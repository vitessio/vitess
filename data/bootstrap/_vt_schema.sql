CREATE DATABASE _vt;

CREATE TABLE _vt.replication_log (
  time_created_ns bigint primary key,
  note varchar(255));

CREATE TABLE _vt.reparent_log (
  time_created_ns bigint primary key,
  last_position varchar(255),
  new_position varchar(255),
  index (last_position));

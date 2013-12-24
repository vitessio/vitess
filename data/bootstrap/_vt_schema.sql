CREATE DATABASE _vt;

CREATE TABLE _vt.replication_log (
  time_created_ns bigint primary key,
  note varchar(255));

CREATE TABLE _vt.reparent_log (
  time_created_ns bigint primary key,
  last_position varchar(255),
  new_addr varchar(255),
  new_position varchar(255),
  wait_position varchar(255),
  index (last_position));

CREATE TABLE _vt.blp_checkpoint (
  source_shard_uid int(10) unsigned NOT NULL,
  group_id bigint default NULL,
  time_updated bigint unsigned NOT NULL,
  PRIMARY KEY (source_shard_uid));

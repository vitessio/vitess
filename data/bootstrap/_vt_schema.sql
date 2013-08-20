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
  addr varchar(255) NOT NULL,
  master_filename varchar(255) NOT NULL,
  master_position bigint(20) unsigned NOT NULL,
  group_id varchar(255) default NULL,
  txn_timestamp int(10) unsigned NOT NULL,
  time_updated int(10) unsigned NOT NULL,
  last_eof_group_id varchar(255) default NULL,
  PRIMARY KEY (source_shard_uid));

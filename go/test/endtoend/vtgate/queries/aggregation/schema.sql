create table t3
(
    id5 bigint,
    id6 bigint,
    id7 bigint,
    primary key (id5)
) Engine = InnoDB;

create table t3_id7_idx
(
    id  bigint not null auto_increment,
    id7 bigint,
    id6 bigint,
    primary key (id)
) Engine = InnoDB;

create table t9
(
    id1 bigint,
    id2 varchar(10),
    id3 varchar(10),
    primary key (id1)
) ENGINE = InnoDB
  DEFAULT charset = utf8mb4
  COLLATE = utf8mb4_general_ci;

create table aggr_test
(
    id   bigint,
    val1 varchar(16),
    val2 bigint,
    primary key (id)
) Engine = InnoDB;

create table aggr_test_dates
(
    id   bigint,
    val1 datetime default current_timestamp,
    val2 datetime default current_timestamp,
    primary key (id)
) Engine = InnoDB;

create table t7_xxhash
(
    uid   varchar(50),
    phone bigint,
    msg   varchar(100),
    primary key (uid)
) Engine = InnoDB;

create table t7_xxhash_idx
(
    phone       bigint,
    keyspace_id varbinary(50),
    primary key (phone, keyspace_id)
) Engine = InnoDB;

CREATE TABLE t1 (
    t1_id bigint unsigned NOT NULL,
    `name` varchar(20) NOT NULL,
    `value` varchar(50),
    shardKey bigint,
    UNIQUE KEY `t1id_name` (t1_id, `name`),
    KEY `IDX_TA_ValueName` (`value`(20), `name`(10))
) ENGINE InnoDB;

CREATE TABLE t2 (
    id bigint NOT NULL,
    shardKey bigint,
    PRIMARY KEY (id)
) ENGINE InnoDB;

CREATE TABLE t11 (
    k BIGINT PRIMARY KEY,
    a INT,
    b INT
);

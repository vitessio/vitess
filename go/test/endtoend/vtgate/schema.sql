create table t1
(
    id1 bigint,
    id2 bigint,
    primary key (id1)
) Engine = InnoDB;

create table t1_id2_idx
(
    id2         bigint,
    keyspace_id varbinary(10),
    primary key (id2)
) Engine = InnoDB;

create table vstream_test
(
    id  bigint,
    val bigint,
    primary key (id)
) Engine = InnoDB;

create table t2
(
    id3 bigint,
    id4 bigint,
    primary key (id3)
) Engine = InnoDB;

create table t2_id4_idx
(
    id  bigint not null auto_increment,
    id4 bigint,
    id3 bigint,
    primary key (id),
    key idx_id4 (id4)
) Engine = InnoDB;

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

create table t4
(
    id1 bigint,
    id2 varchar(10),
    primary key (id1)
) ENGINE = InnoDB
  DEFAULT charset = utf8mb4
  COLLATE = utf8mb4_general_ci;

create table t4_id2_idx
(
    id2         varchar(10),
    id1         bigint,
    keyspace_id varbinary(50),
    primary key (id2, id1)
) Engine = InnoDB
  DEFAULT charset = utf8mb4
  COLLATE = utf8mb4_general_ci;

create table t5_null_vindex
(
    id  bigint not null,
    idx varchar(50),
    primary key (id)
) Engine = InnoDB;

create table t6
(
    id1 bigint,
    id2 varchar(10),
    primary key (id1)
) Engine = InnoDB;

create table t6_id2_idx
(
    id2         varchar(10),
    id1         bigint,
    keyspace_id varbinary(50),
    primary key (id1),
    key (id2)
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

create table t7_fk
(
    id     bigint,
    t7_uid varchar(50),
    primary key (id),
    CONSTRAINT t7_fk_ibfk_1 foreign key (t7_uid) references t7_xxhash (uid)
        on delete set null on update cascade
) Engine = InnoDB;

create table t8
(
    id        bigint,
    t9_id     bigint DEFAULT NULL,
    parent_id bigint,
    primary key (id)
) Engine = InnoDB;

create table t9
(
    id        bigint,
    parent_id bigint,
    primary key (id)
) Engine = InnoDB;

create table t9_id_to_keyspace_id_idx
(
    id          bigint,
    keyspace_id varbinary(10),
    primary key (id)
) Engine = InnoDB;

create table t10
(
    id           bigint,
    sharding_key bigint,
    primary key (id)
) Engine = InnoDB;

create table t10_id_to_keyspace_id_idx
(
    id          bigint,
    keyspace_id varbinary(10),
    primary key (id)
) Engine = InnoDB;

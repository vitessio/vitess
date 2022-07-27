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

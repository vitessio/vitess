create table s_tbl
(
    id  bigint,
    num bigint,
    primary key (id)
) Engine = InnoDB;

create table num_vdx_tbl
(
    num         bigint,
    keyspace_id varbinary(20),
    primary key (num)
) Engine = InnoDB;

create table user_tbl
(
    id        bigint,
    region_id bigint,
    name      varchar(50),
    primary key (id)
) Engine = InnoDB;

create table order_tbl
(
    oid       bigint,
    region_id bigint,
    cust_no   bigint unique key,
    primary key (oid, region_id)
) Engine = InnoDB;

create table oid_vdx_tbl
(
    oid         bigint,
    keyspace_id varbinary(20),
    primary key (oid)
) Engine = InnoDB;

create table oevent_tbl
(
    oid   bigint,
    ename varchar(20),
    primary key (oid, ename)
) Engine = InnoDB;

create table oextra_tbl
(
    id  bigint,
    oid varchar(20),
    primary key (id)
) Engine = InnoDB;

create table auto_tbl
(
    id         bigint,
    unq_col    bigint,
    nonunq_col bigint,
    primary key (id),
    unique (unq_col)
) Engine = InnoDB;

create table unq_idx
(
    unq_col     bigint,
    keyspace_id varbinary(20),
    primary key (unq_col)
) Engine = InnoDB;

create table nonunq_idx
(
    nonunq_col  bigint,
    id          bigint,
    keyspace_id varbinary(20),
    primary key (nonunq_col, id)
) Engine = InnoDB;
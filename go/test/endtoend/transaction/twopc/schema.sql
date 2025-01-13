create table twopc_user
(
    id   bigint,
    name varchar(64),
    primary key (id)
) Engine=InnoDB;

create table twopc_music
(
    id      varchar(64),
    user_id bigint,
    title   varchar(64),
    primary key (id)
) Engine=InnoDB;

create table twopc_t1
(
    id  bigint,
    col bigint,
    primary key (id)
) Engine=InnoDB;

create table twopc_lookup
(
    id  bigint,
    col bigint,
    col_unique bigint,
    primary key (id)
) Engine=InnoDB;

create table lookup
(
    col         bigint,
    id          bigint,
    keyspace_id varbinary(100),
    primary key (col, id)
) Engine = InnoDB;

create table lookup_unique
(
    col_unique    bigint,
    keyspace_id   varbinary(100),
    primary key (col_unique)
) Engine = InnoDB;

create table twopc_consistent_lookup
(
    id  bigint,
    col bigint,
    col_unique bigint,
    primary key (id)
) Engine=InnoDB;

create table consistent_lookup
(
    col         bigint,
    id          bigint,
    keyspace_id varbinary(100),
    primary key (col, id)
) Engine = InnoDB;

create table consistent_lookup_unique
(
    col_unique    bigint,
    keyspace_id   varbinary(100),
    primary key (col_unique)
) Engine = InnoDB;

create table t1
(
    id1 bigint,
    id2 bigint,
    primary key (id1)
) Engine=InnoDB;

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

create table tbl
(
    id         bigint,
    unq_col    bigint,
    nonunq_col bigint,
    primary key (id),
    unique (unq_col)
) Engine = InnoDB;

create table tbl_enum_set
(
    id       bigint,
    enum_col enum('xsmall', 'small', 'medium', 'large', 'xlarge'),
    set_col set('a', 'b', 'c', 'd', 'e', 'f', 'g'),
    primary key (id)
) Engine = InnoDB;

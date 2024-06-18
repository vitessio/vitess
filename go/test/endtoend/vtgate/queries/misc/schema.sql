create table t1
(
    id1 bigint,
    id2 bigint,
    primary key (id1)
) Engine=InnoDB;

create table tbl
(
    id         bigint,
    unq_col    bigint,
    nonunq_col bigint,
    primary key (id),
    unique (unq_col)
) Engine = InnoDB;

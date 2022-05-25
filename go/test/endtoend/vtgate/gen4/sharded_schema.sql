create table t1
(
    id  bigint,
    col bigint,
    primary key (id)
) Engine = InnoDB;

create table t2
(
    id    bigint,
    tcol1 varchar(50),
    tcol2 varchar(50),
    primary key (id)
) Engine = InnoDB;

create table t3
(
    id    bigint,
    tcol1 varchar(50),
    tcol2 varchar(50),
    primary key (id)
) Engine = InnoDB;

create table user_region
(
    id   bigint,
    cola bigint,
    colb bigint,
    primary key (id)
) Engine = InnoDB;

create table region_tbl
(
    rg  bigint,
    uid bigint,
    msg varchar(50),
    primary key (uid)
) Engine = InnoDB;

create table multicol_tbl
(
    cola bigint,
    colb varbinary(50),
    colc varchar(50),
    msg  varchar(50),
    primary key (cola, colb, colc)
) Engine = InnoDB;

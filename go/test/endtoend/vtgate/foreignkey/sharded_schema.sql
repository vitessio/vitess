create table t1
(
    id  bigint,
    col bigint,
    primary key (id)
) Engine = InnoDB;

create table t2
(
    id    bigint,
    col   bigint,
    primary key (id),
    foreign key (id) references t1 (id) on delete restrict
) Engine = InnoDB;

create table t3
(
    id    bigint,
    col   bigint,
    primary key (id),
    foreign key (col) references t1 (id) on delete restrict
) Engine = InnoDB;

create table multicol_tbl1
(
    cola bigint,
    colb varbinary(50),
    colc varchar(50),
    msg  varchar(50),
    primary key (cola, colb, colc)
) Engine = InnoDB;

create table multicol_tbl2
(
    cola bigint,
    colb varbinary(50),
    colc varchar(50),
    msg  varchar(50),
    primary key (cola, colb, colc),
    foreign key (cola, colb, colc) references multicol_tbl1 (cola, colb, colc) on delete cascade
) Engine = InnoDB;

create table t4
(
    id    bigint,
    col   bigint,
    primary key (id),
    foreign key (id) references t2 (id) on delete restrict
) Engine = InnoDB;

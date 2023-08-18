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
    mycol varchar(50),
    primary key (id),
    index(id, mycol),
    index(id, col),
    foreign key (id) references t1 (id) on delete restrict
) Engine = InnoDB;

create table t3
(
    id  bigint,
    col bigint,
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
    id       bigint,
    col      bigint,
    t2_mycol varchar(50),
    t2_col   bigint,
    primary key (id),
    foreign key (id) references t2 (id) on delete restrict,
    foreign key (id, t2_mycol) references t2 (id, mycol) on update restrict,
    foreign key (id, t2_col) references t2 (id, col) on update cascade
) Engine = InnoDB;

create table t5
(
    pk   bigint,
    sk   bigint,
    col1 varchar(50),
    primary key (pk),
    index(sk, col1)
) Engine = InnoDB;

create table t6
(
    pk   bigint,
    sk   bigint,
    col1 varchar(50),
    primary key (pk),
    foreign key (sk, col1) references t5 (sk, col1) on delete restrict on update restrict
) Engine = InnoDB;

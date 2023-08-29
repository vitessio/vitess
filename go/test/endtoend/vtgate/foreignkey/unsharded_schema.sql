create table u_t1
(
    id bigint,
    col1 bigint,
    index(col1),
    primary key (id)
) Engine = InnoDB;

create table u_t2
(
    id bigint,
    col2 bigint,
    primary key (id),
    foreign key (col2) references u_t1 (col1) on delete set null on update set null
) Engine = InnoDB;

create table u_t3
(
    id bigint,
    col3 bigint,
    primary key (id),
    foreign key (col3) references u_t1 (col1) on delete cascade on update cascade
) Engine = InnoDB;

create table fk_t1
(
    id bigint,
    col varchar(10),
    primary key (id),
    index(col)
) Engine = InnoDB;

create table fk_t2
(
    id bigint,
    col varchar(10),
    primary key (id),
    index(col),
    foreign key (col) references fk_t1(col) on delete restrict on update restrict
) Engine = InnoDB;

create table fk_t3
(
    id bigint,
    col varchar(10),
    primary key (id),
    index(col),
    foreign key (col) references fk_t2(col) on delete set null on update set null
) Engine = InnoDB;

create table fk_t4
(
    id bigint,
    col varchar(10),
    primary key (id),
    index(col),
    foreign key (col) references fk_t3(col) on delete set null on update set null
) Engine = InnoDB;

create table fk_t5
(
    id bigint,
    col varchar(10),
    primary key (id),
    index(col),
    foreign key (col) references fk_t4(col) on delete restrict on update restrict
) Engine = InnoDB;

create table fk_t6
(
    id bigint,
    col varchar(10),
    primary key (id),
    index(col),
    foreign key (col) references fk_t3(col) on delete set null on update set null
) Engine = InnoDB;

create table fk_t7
(
    id bigint,
    col varchar(10),
    primary key (id),
    index(col),
    foreign key (col) references fk_t2(col) on delete set null on update set null
) Engine = InnoDB;

create table fk_t10
(
    id bigint,
    col varchar(10),
    primary key (id),
    index(col)
) Engine = InnoDB;

create table fk_t11
(
    id bigint,
    col varchar(10),
    primary key (id),
    index(col),
    foreign key (col) references fk_t10(col) on delete cascade on update cascade
) Engine = InnoDB;

create table fk_t12
(
    id bigint,
    col varchar(10),
    primary key (id),
    index(col),
    foreign key (col) references fk_t11(col) on delete cascade on update cascade
) Engine = InnoDB;

create table fk_t13
(
    id bigint,
    col varchar(10),
    primary key (id),
    index(col),
    foreign key (col) references fk_t11(col) on delete restrict on update restrict
) Engine = InnoDB;


create table fk_t15
(
    id bigint,
    col varchar(10),
    primary key (id),
    index(col)
) Engine = InnoDB;

create table fk_t16
(
    id bigint,
    col varchar(10),
    primary key (id),
    index(col),
    foreign key (col) references fk_t15(col) on delete cascade on update cascade
) Engine = InnoDB;

create table fk_t17
(
    id bigint,
    col varchar(10),
    primary key (id),
    index(col),
    foreign key (col) references fk_t16(col) on delete set null on update set null
) Engine = InnoDB;

create table fk_t18
(
    id bigint,
    col varchar(10),
    primary key (id),
    index(col),
    foreign key (col) references fk_t17(col) on delete cascade on update cascade
) Engine = InnoDB;

create table fk_t19
(
    id bigint,
    col varchar(10),
    primary key (id),
    index(col),
    foreign key (col) references fk_t17(col) on delete set null on update set null
) Engine = InnoDB;
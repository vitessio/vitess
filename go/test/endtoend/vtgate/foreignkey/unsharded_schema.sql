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

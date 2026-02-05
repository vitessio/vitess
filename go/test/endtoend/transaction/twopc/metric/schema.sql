create table twopc_user
(
    id   bigint,
    name varchar(64),
    primary key (id)
) Engine=InnoDB;

create table twopc_t1
(
    id  bigint,
    col bigint,
    primary key (id)
) Engine=InnoDB;
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
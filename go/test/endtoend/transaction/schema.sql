create table twopc_user (
    user_id bigint,
    name varchar(128),
    primary key (user_id)
) Engine=InnoDB;

create table twopc_lookup (
    name varchar(128),
    id bigint,
    primary key (id)
) Engine=InnoDB;

create table test (
    id bigint,
    msg varchar(25),
    primary key (id)
) Engine=InnoDB;
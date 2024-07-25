create table twopc_user (
    id bigint,
    name varchar(64),
    primary key (id)
) Engine=InnoDB;

create table twopc_music (
    id varchar(64),
    user_id bigint,
    title varchar(64),
    primary key (id)
) Engine=InnoDB;

create table twopc_fuzzer_update (
    id bigint,
    col bigint,
    primary key (id)
) Engine=InnoDB;

create table twopc_fuzzer_insert (
    id bigint,
    updateSet bigint,
    col bigint auto_increment,
    primary key (id, col)
) Engine=InnoDB;

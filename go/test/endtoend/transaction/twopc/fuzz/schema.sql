create table twopc_fuzzer_update (
    id bigint,
    col bigint,
    primary key (id)
) Engine=InnoDB;

create table twopc_fuzzer_insert (
    id bigint,
    updateSet bigint,
    threadId bigint,
    col bigint auto_increment,
    key(col),
    primary key (id, col)
) Engine=InnoDB;

create table twopc_t1 (
    id bigint,
    col bigint,
    primary key (id)
) Engine=InnoDB;

create table twopc_t1 (
    id bigint,
    col bigint,
    primary key (id)
) Engine=InnoDB;

create table twopc_settings (
    id bigint,
    col varchar(50),
    primary key (id)
) Engine=InnoDB;

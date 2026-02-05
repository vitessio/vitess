create table test
(
    id    bigint      not null,
    msg   varchar(50) not null,
    extra varchar(100),
    primary key (id),
    index(msg)
) ENGINE=InnoDB;

create table test_idx
(
    msg         varchar(50) not null,
    id          bigint      not null,
    keyspace_id varbinary(50),
    primary key (msg, id)
) ENGINE=InnoDB;
create table user
(
    id            bigint,
    lookup        varchar(128),
    lookup_unique varchar(128),
    primary key (id)
) Engine = InnoDB;

create table lookup
(
    lookup      varchar(128),
    id          bigint,
    keyspace_id varbinary(100),
    primary key (id)
) Engine = InnoDB;

create table lookup_unique
(
    lookup_unique varchar(128),
    keyspace_id   varbinary(100),
    primary key (lookup_unique)
) Engine = InnoDB;
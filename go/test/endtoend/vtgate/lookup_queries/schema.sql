create table user
(
    user_id bigint,
    name    varchar(128),
    primary key (user_id)
) Engine = InnoDB;

create table lookup
(
    name varchar(128),
    id   bigint,
    primary key (id)
) Engine = InnoDB;
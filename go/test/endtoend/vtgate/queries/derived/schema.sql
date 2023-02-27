create table user
(
    id            bigint,
    name varchar(255),
    primary key (id)
) Engine = InnoDB;

create table music
(
    id            bigint,
    user_id bigint,
    primary key (id)
) Engine = InnoDB;

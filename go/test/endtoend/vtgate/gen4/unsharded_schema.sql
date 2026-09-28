create table u_a
(
    id bigint,
    a  bigint,
    primary key (id)
) Engine = InnoDB;

create table u_b
(
    id bigint,
    b  varchar(50),
    primary key (id)
) Engine = InnoDB;

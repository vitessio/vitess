create table user_seq
(
    id      int    default 0,
    next_id bigint default null,
    cache   bigint default null,
    primary key (id)
) comment 'vitess_sequence' Engine = InnoDB;

create table auto_seq
(
    id      int    default 0,
    next_id bigint default null,
    cache   bigint default null,
    primary key (id)
) comment 'vitess_sequence' Engine = InnoDB;

create table mixed_seq
(
    id      int    default 0,
    next_id bigint default null,
    cache   bigint default null,
    primary key (id)
) comment 'vitess_sequence' Engine = InnoDB;

create table u_tbl
(
    id  bigint,
    num bigint,
    primary key (id)
) Engine = InnoDB;

insert into user_seq(id, next_id, cache)
values (0, 1, 1000);
insert into auto_seq(id, next_id, cache)
values (0, 666, 1000);
insert into mixed_seq(id, next_id, cache)
values (0, 1, 1000);
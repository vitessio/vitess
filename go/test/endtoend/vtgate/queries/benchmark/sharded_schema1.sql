create table tbl_no_lkp_vdx
(
    id  bigint,
    c1  varchar(50),
    c2  varchar(50),
    c3  varchar(50),
    c4  varchar(50),
    c5  varchar(50),
    c6  varchar(50),
    c7  varchar(50),
    c8  varchar(50),
    c9  varchar(50),
    c10 varchar(50),
    c11 varchar(50),
    c12 varchar(50)
) Engine = InnoDB;

create table user
(
    id               bigint,
    not_sharding_key bigint,
    type             int,
    team_id          int,
    primary key (id)
);

create table user_extra
(
    id               bigint,
    not_sharding_key bigint,
    col              varchar(50),
    primary key (id)
);

create table mirror_tbl1
(
    id bigint not null,
    primary key(id)
) Engine = InnoDB;

create table mirror_tbl2
(
    id bigint not null,
    primary key(id)
) Engine = InnoDB;

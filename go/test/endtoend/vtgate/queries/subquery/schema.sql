create table t1
(
    id1 bigint,
    id2 bigint,
    primary key (id1)
) Engine = InnoDB;

create table t1_id2_idx
(
    id2         bigint,
    keyspace_id varbinary(10),
    primary key (id2)
) Engine = InnoDB;

create table t2
(
    id3 bigint,
    id4 bigint,
    primary key (id3)
) Engine = InnoDB;

create table t2_id4_idx
(
    id  bigint not null auto_increment,
    id4 bigint,
    id3 bigint,
    primary key (id),
    key idx_id4 (id4)
) Engine = InnoDB;

CREATE TABLE user
(
    id   INT PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE user_extra
(
    user_id    INT,
    extra_info VARCHAR(100),
    PRIMARY KEY (user_id, extra_info)
);
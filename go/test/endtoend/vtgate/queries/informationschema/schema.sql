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

create table t7_xxhash
(
    uid   varchar(50),
    phone bigint,
    msg   varchar(100),
    primary key (uid)
) Engine = InnoDB;

create table t7_xxhash_idx
(
    phone       bigint,
    keyspace_id varbinary(50),
    primary key (phone, keyspace_id)
) Engine = InnoDB;

create table t7_fk
(
    id     bigint,
    t7_uid varchar(50),
    primary key (id),
    CONSTRAINT t7_fk_ibfk_1 foreign key (t7_uid) references t7_xxhash (uid)
        on delete set null on update cascade
) Engine = InnoDB;

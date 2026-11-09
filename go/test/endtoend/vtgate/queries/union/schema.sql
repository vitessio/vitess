create table t1(
                   id1 bigint,
                   id2 bigint,
                   gbname varchar(255) character set gb18030 collate gb18030_bin,
                   primary key(id1)
) Engine=InnoDB;

create table t1_id2_idx(
                           id2 bigint,
                           keyspace_id varbinary(10),
                           primary key(id2)
) Engine=InnoDB;

create table t2(
                   id3 bigint,
                   id4 bigint,
                   primary key(id3)
) Engine=InnoDB;

create table t2_id4_idx(
                           id bigint not null auto_increment,
                           id4 bigint,
                           id3 bigint,
                           primary key(id),
                           key idx_id4(id4)
) Engine=InnoDB;

create table ref(
                   id bigint,
                   primary key(id)
) Engine=InnoDB;

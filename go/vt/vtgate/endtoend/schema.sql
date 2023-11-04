create table t1(
                   id1 bigint,
                   id2 bigint,
                   primary key(id1)
) Engine=InnoDB;

create table t1_copy_basic(
                              id1 bigint,
                              id2 bigint,
                              primary key(id1)
) Engine=InnoDB;

create table t1_copy_all(
                            id1 bigint,
                            id2 bigint,
                            primary key(id1)
) Engine=InnoDB;

create table t1_copy_resume(
                               id1 bigint,
                               id2 bigint,
                               primary key(id1)
) Engine=InnoDB;

create table t1_id2_idx(
                           id2 bigint,
                           keyspace_id varbinary(10),
                           primary key(id2)
) Engine=InnoDB;

create table vstream_test(
                             id bigint,
                             val bigint,
                             primary key(id)
) Engine=InnoDB;

create table aggr_test(
                          id bigint,
                          val1 varchar(16),
                          val2 bigint,
                          primary key(id)
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

create table t1_last_insert_id(
                                  id bigint not null auto_increment,
                                  id1 bigint,
                                  primary key(id)
) Engine=InnoDB;

create table t1_row_count(
                             id bigint not null,
                             id1 bigint,
                             primary key(id)
) Engine=InnoDB;

create table t1_sharded(
                           id1 bigint,
                           id2 bigint,
                           primary key(id1)
) Engine=InnoDB;

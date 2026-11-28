create table t1(
                   id1 bigint,
                   id2 bigint,
                   primary key(id1)
) Engine=InnoDB;

create table union_collations(
    id bigint primary key,
    name_general varchar(30) character set utf8mb4 collate utf8mb4_general_ci,
    name_bin varchar(30) character set utf8mb4 collate utf8mb4_bin,
    name_latin1 varchar(30) character set latin1 collate latin1_swedish_ci,
    name_ascii varchar(30) character set ascii collate ascii_general_ci,
    name_text text character set utf8mb4 collate utf8mb4_general_ci,
    name_blob blob,
    data_json json,
    enum_sort enum('b', 'a') character set utf8mb4 collate utf8mb4_0900_ai_ci
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

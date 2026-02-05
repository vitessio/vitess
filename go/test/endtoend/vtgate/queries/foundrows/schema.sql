create table t2
(
    id3 bigint,
    id4 bigint,
    primary key (id3)
) Engine=InnoDB;

create table t2_id4_idx
(
    id  bigint not null auto_increment,
    id4 bigint,
    id3 bigint,
    primary key (id),
    key idx_id4(id4)
) Engine=InnoDB;

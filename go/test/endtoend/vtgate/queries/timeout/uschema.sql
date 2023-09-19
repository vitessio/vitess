create table unsharded(
    id1 bigint,
    id2 bigint,
    key(id1)
) Engine=InnoDB;
create /*vt+ QUERY_TIMEOUT_MS=1000 */ table if not exists t1(
                   id1 bigint,
                   id2 bigint,
                   primary key(id1)
) Engine=InnoDB;
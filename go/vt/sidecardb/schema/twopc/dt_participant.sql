CREATE TABLE IF NOT EXISTS _vt.dt_participant
(
    dtid varbinary(512) NOT NULL,
    id bigint NOT NULL,
    keyspace varchar(256) NOT NULL,
    shard varchar(256) NOT NULL,
    primary key(dtid, id)
) ENGINE = InnoDB

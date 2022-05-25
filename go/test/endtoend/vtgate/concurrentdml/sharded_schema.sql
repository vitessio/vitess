CREATE TABLE t1
(
    c1 BIGINT NOT NULL,
    c2 BIGINT NOT NULL,
    c3 BIGINT,
    c4 varchar(100),
    PRIMARY KEY (c1),
    UNIQUE KEY (c2),
    UNIQUE KEY (c3),
    UNIQUE KEY (c4)
) ENGINE = Innodb;

CREATE TABLE lookup_t1
(
    c2          BIGINT NOT NULL,
    keyspace_id BINARY(8),
    primary key (c2)
);

CREATE TABLE lookup_t2
(
    c3          BIGINT NOT NULL,
    keyspace_id BINARY(8),
    primary key (c3)
);

CREATE TABLE lookup_t3
(
    c4          varchar(100) NOT NULL,
    keyspace_id BINARY(8),
    primary key (c4)
);

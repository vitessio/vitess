CREATE TABLE t1 (
                    c1 bigint unsigned NOT NULL,
                    c2 varchar(16) NOT NULL,
                    PRIMARY KEY (c1)
) ENGINE InnoDB;

CREATE TABLE lookup_t1 (
                           c2 varchar(16) NOT NULL,
                           keyspace_id varbinary(16) NOT NULL,
                           PRIMARY KEY (c2)
) ENGINE InnoDB;

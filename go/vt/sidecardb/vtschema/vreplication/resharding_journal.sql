CREATE TABLE _vt.resharding_journal
(
    `id`      bigint NOT NULL,
    `db_name` varbinary(255) DEFAULT NULL,
    `val`     blob,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB

CREATE TABLE _vt.vdiff_table
(
    `vdiff_id`      varchar(64)    NOT NULL,
    `table_name`    varbinary(128) NOT NULL,
    `state`         varbinary(64)           DEFAULT NULL,
    `lastpk`        varbinary(2000)         DEFAULT NULL,
    `table_rows`    bigint(20)     NOT NULL DEFAULT '0',
    `rows_compared` bigint(20)     NOT NULL DEFAULT '0',
    `mismatch`      tinyint(1)     NOT NULL DEFAULT '0',
    `report`        json                    DEFAULT NULL,
    `created_at`    timestamp      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`    timestamp      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`vdiff_id`, `table_name`)
) ENGINE = InnoDB

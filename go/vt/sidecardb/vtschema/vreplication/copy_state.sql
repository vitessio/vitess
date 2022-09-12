CREATE TABLE _vt.copy_state
(
    `vrepl_id`   int            NOT NULL,
    `table_name` varbinary(128) NOT NULL,
    `lastpk`     varbinary(2000) DEFAULT NULL,
    PRIMARY KEY (`vrepl_id`, `table_name`)
) ENGINE = InnoDB

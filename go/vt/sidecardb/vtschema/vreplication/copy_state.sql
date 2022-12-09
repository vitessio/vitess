CREATE TABLE _vt.copy_state
(
    `id` bigint unsigned NOT NULL AUTO_INCREMENT,
    `vrepl_id`   int            NOT NULL,
    `table_name` varbinary(128) NOT NULL,
    `lastpk`     varbinary(2000) DEFAULT NULL,
    PRIMARY KEY (`id`),
    KEY `vrepl_id` (`vrepl_id`,`table_name`)
) ENGINE = InnoDB

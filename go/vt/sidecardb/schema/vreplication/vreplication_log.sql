CREATE TABLE IF NOT EXISTS _vt.vreplication_log
(
    `id`         bigint         NOT NULL AUTO_INCREMENT,
    `vrepl_id`   int            NOT NULL,
    `type`       varbinary(256) NOT NULL,
    `state`      varbinary(100) NOT NULL,
    `created_at` timestamp      NULL     DEFAULT CURRENT_TIMESTAMP,
    `updated_at` timestamp      NULL     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `message`    text           NOT NULL,
    `count`      bigint         NOT NULL DEFAULT '1',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB

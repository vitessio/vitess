CREATE TABLE _vt.vdiff_log
(
    `id`         int(11)   NOT NULL AUTO_INCREMENT,
    `vdiff_id`   int(11)   NOT NULL,
    `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `message`    text      NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB

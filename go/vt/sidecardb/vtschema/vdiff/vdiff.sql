CREATE TABLE IF NOT EXISTS _vt.vdiff
(
    `id`                 bigint(20)   NOT NULL AUTO_INCREMENT,
    `vdiff_uuid`         varchar(64)  NOT NULL,
    `workflow`           varbinary(1024)       DEFAULT NULL,
    `keyspace`           varbinary(256)        DEFAULT NULL,
    `shard`              varchar(255) NOT NULL,
    `db_name`            varbinary(1024)       DEFAULT NULL,
    `state`              varbinary(64)         DEFAULT NULL,
    `options`            json                  DEFAULT NULL,
    `created_at`         timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `started_at`         timestamp    NULL     DEFAULT NULL,
    `liveness_timestamp` timestamp    NULL     DEFAULT NULL,
    `completed_at`       timestamp    NULL     DEFAULT NULL,
    `last_error`         varbinary(512)        DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uuid_idx` (`vdiff_uuid`),
    KEY `state` (`state`),
    KEY `ks_wf_idx` (`keyspace`(64), `workflow`(64))
) ENGINE = InnoDB

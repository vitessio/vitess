CREATE TABLE _vt.reparent_journal
(
    `time_created_ns`      bigint(20) unsigned NOT NULL,
    `action_name`          varbinary(250)      NOT NULL,
    `primary_alias`        varbinary(32)       NOT NULL,
    `replication_position` varbinary(64000) DEFAULT NULL,

    PRIMARY KEY (`time_created_ns`)
) ENGINE = InnoDB

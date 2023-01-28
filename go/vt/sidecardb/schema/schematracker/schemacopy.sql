CREATE TABLE IF NOT EXISTS _vt.schemacopy
(
    `table_schema`       varchar(64)     NOT NULL,
    `table_name`         varchar(64)     NOT NULL,
    `column_name`        varchar(64)     NOT NULL,
    `ordinal_position`   bigint unsigned NOT NULL,
    `character_set_name` varchar(32) DEFAULT NULL,
    `collation_name`     varchar(32) DEFAULT NULL,
    `data_type`          varchar(64)     NOT NULL,
    `column_key`         varchar(3)      NOT NULL,
    PRIMARY KEY (`table_schema`, `table_name`, `ordinal_position`)
) ENGINE = InnoDB

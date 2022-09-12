CREATE TABLE _vt.shard_metadata
(
    `name`    varchar(255)   NOT NULL,
    `value`   mediumblob     NOT NULL,
    `db_name` varbinary(255) NOT NULL,

    PRIMARY KEY (`name`, `db_name`)
) ENGINE = InnoDB

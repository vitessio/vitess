CREATE TABLE IF NOT EXISTS _vt.schema_version
(
    id           INT NOT NULL AUTO_INCREMENT,
    pos          VARBINARY(10000) NOT NULL,
    time_updated BIGINT(20)       NOT NULL,
    ddl          BLOB DEFAULT NULL,
    schemax      LONGBLOB         NOT NULL,
    PRIMARY KEY (id)
) ENGINE = InnoDB

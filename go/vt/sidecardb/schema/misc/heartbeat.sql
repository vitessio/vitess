CREATE TABLE IF NOT EXISTS _vt.heartbeat
(
    keyspaceShard VARBINARY(256)  NOT NULL,
    tabletUid     INT UNSIGNED    NOT NULL,
    ts            BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (`keyspaceShard`)
) engine = InnoDB

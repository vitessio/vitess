CREATE TABLE `unsharded` (
    `id`     INT NOT NULL PRIMARY KEY,
    `col`    VARCHAR(255) DEFAULT NULL,
    `col1`   VARCHAR(255) DEFAULT NULL,
    `col2`   VARCHAR(255) DEFAULT NULL,
    `name`   VARCHAR(255) DEFAULT NULL,
    `baz`    INT
);

CREATE TABLE `unsharded_auto` (
    `id`   INT NOT NULL PRIMARY KEY,
    `col1` VARCHAR(255) DEFAULT NULL,
    `col2` VARCHAR(255) DEFAULT NULL
);

CREATE TABLE `unsharded_a` (
    `id`   INT NOT NULL PRIMARY KEY,
    `col`  VARCHAR(255) DEFAULT NULL,
    `name` VARCHAR(255) DEFAULT NULL
);

CREATE TABLE `unsharded_b` (
    `id`   INT NOT NULL PRIMARY KEY,
    `col`  VARCHAR(255) DEFAULT NULL,
    `name` VARCHAR(255) DEFAULT NULL
);
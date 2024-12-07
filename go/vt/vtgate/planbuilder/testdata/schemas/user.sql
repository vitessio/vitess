CREATE TABLE user
(
    id       INT PRIMARY KEY,
    col      BIGINT,
    predef1  VARCHAR(255),
    predef2  VARCHAR(255),
    textcol1 VARCHAR(255),
    intcol   BIGINT,
    textcol2 VARCHAR(255)
);

CREATE TABLE user_metadata
(
    user_id      INT,
    email        VARCHAR(255),
    address      VARCHAR(255),
    md5          VARCHAR(255),
    non_planable VARCHAR(255),
    PRIMARY KEY (user_id)
);

CREATE TABLE music
(
    user_id INT,
    id      INT,
    PRIMARY KEY (user_id)
);

CREATE TABLE samecolvin
(
    col VARCHAR(255),
    PRIMARY KEY (col)
);

CREATE TABLE multicolvin
(
    kid      INT,
    column_a VARCHAR(255),
    column_b VARCHAR(255),
    column_c VARCHAR(255),
    PRIMARY KEY (kid)
);

CREATE TABLE customer
(
    id    INT,
    email VARCHAR(255),
    phone VARCHAR(255),
    PRIMARY KEY (id)
);

CREATE TABLE multicol_tbl
(
    cola VARCHAR(255),
    colb VARCHAR(255),
    colc VARCHAR(255),
    name VARCHAR(255),
    PRIMARY KEY (cola, colb)
);

CREATE TABLE mixed_tbl
(
    shard_key VARCHAR(255),
    lkp_key   VARCHAR(255),
    PRIMARY KEY (shard_key)
);

CREATE TABLE pin_test
(
    id INT PRIMARY KEY
);

CREATE TABLE cfc_vindex_col
(
    c1 VARCHAR(255),
    c2 VARCHAR(255),
    PRIMARY KEY (c1)
);

CREATE TABLE unq_lkp_idx
(
    unq_key     INT PRIMARY KEY,
    keyspace_id VARCHAR(255)
);

CREATE TABLE t1
(
    c1 INT,
    c2 INT,
    c3 INT,
    PRIMARY KEY (c1)
);

CREATE TABLE authoritative
(
    user_id bigint NOT NULL,
    col1    VARCHAR(255),
    col2    bigint,
    PRIMARY KEY (user_id)
) ENGINE=InnoDB;
CREATE TABLE user
(
    id         INT PRIMARY KEY,
    col        BIGINT,
    intcol     BIGINT,
    user_id    INT,
    id1        INT,
    id2        INT,
    id3        INT,
    m          INT,
    bar        INT,
    a          INT,
    name       VARCHAR(255),
    col1       VARCHAR(255),
    col2       VARCHAR(255),
    costly     VARCHAR(255),
    predef1    VARCHAR(255),
    predef2    VARCHAR(255),
    textcol1   VARCHAR(255),
    textcol2   VARCHAR(255),
    someColumn VARCHAR(255),
    foo        VARCHAR(255)
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
    user_id   INT,
    id        INT,
    col       VARCHAR(255),
    col1      VARCHAR(255),
    col2      VARCHAR(255),
    genre     VARCHAR(255),
    componist VARCHAR(255),
    PRIMARY KEY (user_id)
);

CREATE TABLE name_user_vdx
(
    name        INT,
    keyspace_id VARBINARY(10),
    primary key (name)
);

CREATE TABLE samecolvin
(
    col VARCHAR(255),
    PRIMARY KEY (col)
);

CREATE TABLE multicolvin
(
    kid      INT,
    column_a INT,
    column_b INT,
    column_c INT,
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

CREATE TABLE colb_colc_map
(
    colb        INT PRIMARY KEY,
    colc        INT,
    keyspace_id VARCHAR(255)
);

CREATE TABLE seq
(
    id      INT,
    next_id BIGINT,
    cache   BIGINT,
    PRIMARY KEY (id)
) COMMENT 'vitess_sequence';

CREATE TABLE user_extra
(
    id       INT,
    user_id  INT,
    extra_id INT,
    col      INT,
    m2       INT,
    PRIMARY KEY (id, extra_id)
);

CREATE TABLE name_user_map
(
    name        VARCHAR(255),
    keyspace_id VARCHAR(255)
);

CREATE TABLE costly_map
(
    costly      VARCHAR(255),
    keyspace_id VARCHAR(255)
);

CREATE TABLE unq_binary_idx
(
    id   INT PRIMARY KEY,
    col1 INT
);

CREATE TABLE sales
(
    oid  INT PRIMARY KEY,
    col1 VARCHAR(255)
);

CREATE TABLE sales_extra
(
    colx  INT PRIMARY KEY,
    cola  VARCHAR(255),
    colb  VARCHAR(255),
    start INT,
    end   INT
);

CREATE TABLE ref
(
    col INT PRIMARY KEY
);
create table t1
(
    id1 bigint,
    id2 bigint,
    primary key (id1)
) Engine=InnoDB;

create table unq_idx
(
    unq_col     bigint,
    keyspace_id varbinary(20),
    primary key (unq_col)
) Engine = InnoDB;

create table nonunq_idx
(
    nonunq_col  bigint,
    id          bigint,
    keyspace_id varbinary(20),
    primary key (nonunq_col, id)
) Engine = InnoDB;

create table tbl
(
    id         bigint,
    unq_col    bigint,
    nonunq_col bigint not null,
    primary key (id),
    unique (unq_col)
) Engine = InnoDB;

create table tbl_enum_set
(
    id       bigint,
    enum_col enum('xsmall', 'small', 'medium', 'large', 'xlarge'),
    set_col set('a', 'b', 'c', 'd', 'e', 'f', 'g'),
    primary key (id)
) Engine = InnoDB;

create table all_types
(
    id                 bigint not null,
    msg                varchar(64),
    keyspace_id        bigint(20) unsigned,
    tinyint_unsigned   TINYINT,
    bool_signed        BOOL,
    smallint_unsigned  SMALLINT,
    mediumint_unsigned MEDIUMINT,
    int_unsigned       INT,
    float_unsigned     FLOAT(10, 2),
    double_unsigned    DOUBLE(16, 2),
    decimal_unsigned   DECIMAL,
    t_date             DATE,
    t_datetime         DATETIME,
    t_datetime_micros  DATETIME(6),
    t_time             TIME,
    t_timestamp        TIMESTAMP,
    c8                 bit(8)  DEFAULT NULL,
    c16                bit(16) DEFAULT NULL,
    c24                bit(24) DEFAULT NULL,
    c32                bit(32) DEFAULT NULL,
    c40                bit(40) DEFAULT NULL,
    c48                bit(48) DEFAULT NULL,
    c56                bit(56) DEFAULT NULL,
    c63                bit(63) DEFAULT NULL,
    c64                bit(64) DEFAULT NULL,
    json_col           JSON,
    text_col           TEXT,
    data               longblob,
    tinyint_min        TINYINT,
    tinyint_max        TINYINT,
    tinyint_pos        TINYINT,
    tinyint_neg        TINYINT,
    smallint_min       SMALLINT,
    smallint_max       SMALLINT,
    smallint_pos       SMALLINT,
    smallint_neg       SMALLINT,
    medint_min         MEDIUMINT,
    medint_max         MEDIUMINT,
    medint_pos         MEDIUMINT,
    medint_neg         MEDIUMINT,
    int_min            INT,
    int_max            INT,
    int_pos            INT,
    int_neg            INT,
    bigint_min         BIGINT,
    bigint_max         BIGINT,
    bigint_pos         BIGINT,
    bigint_neg         BIGINT,
    primary key (id)
) Engine = InnoDB;

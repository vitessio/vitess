-- Schema for the column-name corpus. Load with:
--   mysql -uroot -h127.0.0.1 -P<port> --default-character-set=utf8mb4 < schema.sql
DROP DATABASE IF EXISTS colnames;
CREATE DATABASE colnames DEFAULT CHARACTER SET utf8mb4;
USE colnames;

CREATE TABLE t (
  id int PRIMARY KEY,
  Col varchar(10) DEFAULT 'dflt',
  `MiXeD` int,
  j json,
  d date
);
INSERT INTO t VALUES (1, 'a', 10, '{"a": 1, "a b": 2}', '2020-01-01'), (2, 'b', 20, '{"a": 2}', '2021-02-03');

CREATE TABLE u (id int, x int);
INSERT INTO u VALUES (1, 100), (3, 300);

-- shares "id" with t (natural join), plus its own column
CREATE TABLE t2 (id int, y int);
INSERT INTO t2 VALUES (1, 1000);

-- same column names as t but different case
CREATE TABLE tt (ID int, col varchar(10));
INSERT INTO tt VALUES (1, 'x');

-- weird column names
CREATE TABLE `we ird` (`a b` int, `c``d` int, `ünï` int, `e"f` int, `select` int, `1x` int, ` lead` int);
INSERT INTO `we ird` VALUES (1, 2, 3, 4, 5, 6, 7);

-- max-length (64 char) column name
CREATE TABLE longcol (`c234567890123456789012345678901234567890123456789012345678901234` int);
INSERT INTO longcol VALUES (1);

CREATE TABLE ft (id int PRIMARY KEY, body text, FULLTEXT KEY (body)) ENGINE=InnoDB;
INSERT INTO ft VALUES (1, 'hello world'), (2, 'other text');

-- views
CREATE VIEW v1 AS SELECT 1+1, concat('a','b'), id, Col FROM t;
CREATE VIEW v2 (a, b) AS SELECT id, Col FROM t;
CREATE VIEW v3 AS SELECT t.id AS tid, u.x FROM t JOIN u ON t.id = u.id;
CREATE VIEW v4 AS SELECT 'lit', 1.50, -1, NULL, id+0 AS `ID plus`, concat('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb') FROM t;
CREATE VIEW v5 AS SELECT ID, COL, mixed FROM t;
CREATE VIEW v6 AS SELECT * FROM t;

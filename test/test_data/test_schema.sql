# init
set storage_engine=InnoDB
create table vtocc_test(intval int, floatval float, charval varchar(256), binval varbinary(256), primary key(intval)) comment 'vtocc_nocache'
begin
delete from vtocc_test
insert into vtocc_test values(1, 1.12345, 0xC2A2, 0x00FF), (2, null, '', null), (3, null, null, null)
commit

create table vtocc_a(eid bigint default 1, id int default 1, name varchar(128), foo varbinary(128), primary key(eid, id)) comment 'vtocc_nocache'
create table vtocc_b(eid bigint, id int, primary key(eid, id)) comment 'vtocc_nocache'
create table vtocc_c(eid bigint, name varchar(128), foo varbinary(128), primary key(eid, name)) comment 'vtocc_nocache'
create table vtocc_d(eid bigint, id int) comment 'vtocc_nocache'
create table vtocc_e(eid bigint auto_increment, id int default 1, name varchar(128) default 'name', foo varchar(128), primary key(eid, id, name)) comment 'vtocc_nocache'
create table vtocc_f(vb varbinary(16) default 'ab', id int, primary key(vb)) comment 'vtocc_nocache'
begin
delete from vtocc_a
delete from vtocc_c
insert into vtocc_a(eid, id, name, foo) values(1, 1, 'abcd', 'efgh'), (1, 2, 'bcde', 'fghi')
insert into vtocc_b(eid, id) values(1, 1), (1, 2)
insert into vtocc_c(eid, name, foo) values(10, 'abcd', '20'), (11, 'bcde', '30')
commit

create table vtocc_cached(eid bigint, bid varbinary(16), name varchar(128), foo varbinary(128), primary key(eid, bid))
create index aname on vtocc_cached(eid, name)
begin
delete from vtocc_cached
insert into vtocc_cached values (1, 'foo', 'abcd1', 'efgh')
insert into vtocc_cached values (1, 'bar', 'abcd1', 'efgh')
insert into vtocc_cached values (2, 'foo', 'abcd2', 'efgh')
insert into vtocc_cached values (2, 'bar', 'abcd2', 'efgh')
commit

create table vtocc_big(id int, string1 varchar(128), string2 varchar(100), string3 char(1), string4 varchar(50), string5 varchar(50), date1 date, string6 varchar(16), string7 varchar(120), bigint1 bigint(20), bigint2 bigint(20), date2 date, integer1 int, tinyint1 tinyint(4), primary key(id)) comment 'vtocc_big'

create table vtocc_ints(tiny tinyint, tinyu tinyint unsigned, small smallint, smallu smallint unsigned, medium mediumint, mediumu mediumint unsigned, normal int, normalu int unsigned, big bigint, bigu bigint unsigned, y year, primary key(tiny)) comment 'vtocc_nocache'
create table vtocc_fracts(id int, deci decimal(5,2), num numeric(5,2), f float, d double, primary key(id)) comment 'vtocc_nocache'
create table vtocc_strings(vb varbinary(16), c char(16), vc varchar(16), b binary(4), tb tinyblob, bl blob, ttx tinytext, tx text, en enum('a','b'), s set('a','b'), primary key(vb)) comment 'vtocc_nocache'
create table vtocc_misc(id int, b bit(8), d date, dt datetime, t time, primary key(id)) comment 'vtocc_nocache'

create table vtocc_part1(key1 bigint, key2 bigint, data1 int, primary key(key1, key2))
create unique index vtocc_key2 on vtocc_part1(key2)
create table vtocc_part2(key3 bigint, data2 int, primary key(key3))
create view vtocc_view as select key2, key1, data1, data2 from vtocc_part1, vtocc_part2 where key2=key3
begin
delete from vtocc_part1
delete from vtocc_part2
insert into vtocc_part1 values(10, 1, 1)
insert into vtocc_part1 values(10, 2, 2)
insert into vtocc_part2 values(1, 3)
insert into vtocc_part2 values(2, 4)
commit

# clean
drop table vtocc_test
drop table vtocc_a
drop table vtocc_b
drop table vtocc_c
drop table vtocc_d
drop table vtocc_e
drop table vtocc_f
drop table vtocc_cached
drop table vtocc_cached2
drop table vtocc_nocache
drop table vtocc_big
drop table vtocc_ints
drop table vtocc_fracts
drop table vtocc_strings
drop table vtocc_misc
drop view vtocc_view
drop table vtocc_part1
drop table vtocc_part2

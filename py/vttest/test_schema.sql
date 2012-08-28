# init
set storage_engine=InnoDB
create table vtocc_test(intval int, floatval float, charval varchar(256), binval varbinary(256), primary key(intval)) comment 'vtocc_nocache'
begin
delete from vtocc_test
insert into vtocc_test values(1, 1.12345, 0xC2A2, 0x00FF), (2, null, '', null), (3, null, null, null)
commit

create table vtocc_a(eid bigint, id int, name varchar(128), foo varbinary(128), primary key(eid, id)) comment 'vtocc_nocache'
create table vtocc_b(eid bigint, id int, primary key(eid, id)) comment 'vtocc_nocache'
create table vtocc_c(eid bigint, name varchar(128), foo varbinary(128), primary key(eid, name)) comment 'vtocc_nocache'
create table vtocc_d(eid bigint, id int) comment 'vtocc_nocache'
create table vtocc_e(eid bigint auto_increment, id int default 1, name varchar(128) default 'name', foo varchar(128), primary key(eid, id, name)) comment 'vtocc_nocache'
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

# clean
drop table vtocc_test
drop table vtocc_a
drop table vtocc_b
drop table vtocc_c
drop table vtocc_d
drop table vtocc_e
drop table vtocc_cached
drop table vtocc_cached2
drop table vtocc_nocache

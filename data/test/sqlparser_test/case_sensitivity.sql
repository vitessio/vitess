create table A
create index b on A#alter table A
alter table A foo#alter table A
alter table A rename to B#rename table A B
rename table A to B#rename table A B
drop table B
drop index b on A#alter table A
select a from B
select A as B from C#select a as b from C
select B.* from c
select B.A from c#select B.a from c
select * from B as C
select * from A.B
update A set b = 1
update A.B set b = 1
select A() from b#select a() from b
select A(B, C) from b#select a(b, c) from b
select A(distinct B, C) from b#select a(distinct b, c) from b
select IF(B, C) from b#select if(b, c) from b
select VALUES(B, C) from b#select values(b, c) from b
select * from b use index (A)#select * from b use index (a)
insert into A(A, B) values (1, 2)#insert into A(a, b) values (1, 2)
CREATE TABLE A#create table A
create view A#create table a
alter view A#alter table a
drop view A#drop table a

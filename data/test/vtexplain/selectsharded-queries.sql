select * from user /* scatter */;
select * from user where id = 1 /* equal unique */;
select * from user where id > 100 /* scatter range */;
select * from user where name = 'bob' /* vindex lookup */;
select * from user where name = 'bob' or nickname = 'bob' /* vindex lookup */;

select u.id, u.name, u.nickname, n.info from user u join name_info n on u.name = n.name /* join on varchar */;
select m.id, m.song, e.extra from music m join music_extra e on m.id = e.id where m.user_id = 100 /* join on int */;

select count(*) from user where id = 1 /* point aggregate */;
select count(*) from user where name in ('alice','bob') /* scatter aggregate */;
select name, count(*) from user group by name /* scatter aggregate */;

select 1, "hello", 3.14 from user limit 10 /* select constant sql values */;
select * from (select id from user) s /* scatter paren select */;

select name from user where id = (select id from t1) /* non-correlated subquery as value */;
select name from user where id in (select id from t1) /* non-correlated subquery in IN clause */;
select name from user where id not in (select id from t1) /* non-correlated subquery in NOT IN clause */;
select name from user where exists (select id from t1) /* non-correlated subquery as EXISTS */;

select * from user /* scatter */;
select * from user where id = 1 /* equal unique */;
select * from user where id > 100 /* scatter range */;
select * from user where name = 'bob'/* vindex lookup */;
select * from user where name = 'bob' or nickname = 'bob'/* vindex lookup */;
select u.name, n.info from user u join name_info n on u.name = n.name /* join on varchar */;
select m.id, m.song, e.extra from music m join music_extra e on m.id = e.id where m.user_id = 100 /* join on int */;

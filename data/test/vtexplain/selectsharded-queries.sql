select * from user /* scatter */;
select * from user where id = 1 /* equal unique */;
select * from user where id > 100 /* scatter range */;
select * from user where name = 'bob'/* vindex lookup */;

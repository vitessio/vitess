select * from t1;
insert into t1 (id,intval,floatval) values (1,2,3.14);
update t1 set intval = 10;
update t1 set floatval = 9.99;
delete from t1 where id = 100;
insert into t1 (id,intval,floatval) values (1,2,3.14) on duplicate key update intval=3, floatval=3.14;

select * from t1;
insert into t1 (id,val) values (1,2);
update t1 set val = 10;
delete from t1 where id = 100;
insert into t1 (id,val) values (1,2) on duplicate key update val=3;

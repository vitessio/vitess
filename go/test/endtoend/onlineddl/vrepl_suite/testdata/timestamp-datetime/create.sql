drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  i int not null,
  ts timestamp default current_timestamp,
  dt datetime,
  ts2ts timestamp null,
  ts2dt datetime null,
  dt2ts timestamp null,
  dt2dt datetime null,
  updated tinyint unsigned default 0,
  primary key(id)
) auto_increment=1;

drop event if exists onlineddl_test;
delimiter ;;
create event onlineddl_test
  on schedule every 1 second
  starts current_timestamp
  ends current_timestamp + interval 60 second
  on completion not preserve
  enable
  do
begin
  insert into onlineddl_test values (null, 11, now(), now(),null, null, null, null,  0);
  update onlineddl_test set ts2ts=ts, ts2dt=ts, dt2ts=dt, dt2dt=dt where i = 11 order by id desc limit 1;

  insert into onlineddl_test values (null, 13, null, now(), now(), 0);
  update onlineddl_test set ts2ts=ts, ts2dt=ts, dt2ts=dt, dt2dt=dt where i = 13 order by id desc limit 1;

  insert into onlineddl_test values (null, 17, null, '2016-07-06 10:20:30', '2016-07-06 10:20:30', 0);
  update onlineddl_test set ts2ts=ts, ts2dt=ts, dt2ts=dt, dt2dt=dt where i = 17 order by id desc limit 1;
end ;;

drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  i int not null,
  ts0 timestamp default current_timestamp,
  ts1 timestamp default current_timestamp,
  ts2 timestamp default current_timestamp,
  updated tinyint unsigned default 0,
  primary key(id),
  key i_idx(i)
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
  insert into onlineddl_test values (null, 11, null, now(), now(), 0);
  update onlineddl_test set ts2=now() + interval 10 minute, updated = 1 where i = 11 order by id desc limit 1;

  set session time_zone='system';
  insert into onlineddl_test values (null, 13, null, now(), now(), 0);
  update onlineddl_test set ts2=now() + interval 10 minute, updated = 1 where i = 13 order by id desc limit 1;

  set session time_zone='+00:00';
  insert into onlineddl_test values (null, 17, null, now(), now(), 0);
  update onlineddl_test set ts2=now() + interval 10 minute, updated = 1 where i = 17 order by id desc limit 1;

  set session time_zone='-03:00';
  insert into onlineddl_test values (null, 19, null, now(), now(), 0);
  update onlineddl_test set ts2=now() + interval 10 minute, updated = 1 where i = 19 order by id desc limit 1;

  set session time_zone='+05:00';
  insert into onlineddl_test values (null, 23, null, now(), now(), 0);
  update onlineddl_test set ts2=now() + interval 10 minute, updated = 1 where i = 23 order by id desc limit 1;
end ;;

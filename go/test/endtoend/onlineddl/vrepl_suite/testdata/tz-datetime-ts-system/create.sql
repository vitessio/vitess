set @original_timezone=@@global.time_zone;

drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  i int not null,
  ts0 timestamp default current_timestamp,
  ts1 timestamp default current_timestamp,
  dt2 datetime,
  t   datetime,
  updated tinyint unsigned default 0,
  primary key(id),
  key i_idx(i)
) auto_increment=1;

insert into onlineddl_test values (null, 2, null, now(), now(), '2010-10-20 10:20:30', 0);
insert into onlineddl_test values (null, 3, null, now(), now(), now(), 0);

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
  set @@global.TIME_ZONE='+05:00';

  set session time_zone='+02:00';
  insert into onlineddl_test values (null, 7, null, now(), now(), '2010-10-20 10:20:30', 0);
  insert into onlineddl_test values (null, 8, null, now(), now(), now(), 0);

  insert into onlineddl_test values (null, 11, null, now(), now(), '2010-10-20 10:20:30', 0);
  update onlineddl_test set ts2=now() + interval 1 minute, updated = 1 where i = 11 order by id desc limit 1;

  set session time_zone='system';
  insert into onlineddl_test values (null, 13, null, now(), now(), '2010-10-20 10:20:30', 0);
  update onlineddl_test set ts2=now() + interval 1 minute, updated = 1 where i = 13 order by id desc limit 1;
  insert into onlineddl_test values (null, 14, null, now(), now(), now(), 0);
  update onlineddl_test set ts2=now() + interval 1 minute, updated = 1 where i = 14 order by id desc limit 1;

  set session time_zone='+00:00';
  insert into onlineddl_test values (null, 17, null, now(), now(), '2010-10-20 10:20:30', 0);
  update onlineddl_test set ts2=now() + interval 1 minute, updated = 1 where i = 17 order by id desc limit 1;
  insert into onlineddl_test values (null, 18, null, now(), now(), now(), 0);
  update onlineddl_test set ts2=now() + interval 1 minute, updated = 1 where i = 18 order by id desc limit 1;

  set session time_zone='-03:00';
  insert into onlineddl_test values (null, 19, null, now(), now(), '2010-10-20 10:20:30', 0);
  update onlineddl_test set ts2=now() + interval 1 minute, updated = 1 where i = 19 order by id desc limit 1;

  set session time_zone='+05:00';
  insert into onlineddl_test values (null, 23, null, now(), now(), '2010-10-20 10:20:30', 0);
  update onlineddl_test set ts2=now() + interval 1 minute, updated = 1 where i = 23 order by id desc limit 1;
  insert into onlineddl_test values (null, 24, null, now(), now(), now(), 0);
  update onlineddl_test set ts2=now() + interval 1 minute, updated = 1 where i = 24 order by id desc limit 1;

  set @@global.TIME_ZONE=@original_timezone;
end ;;

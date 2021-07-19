drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  dec0 decimal(65,30) unsigned NOT NULL DEFAULT '0.000000000000000000000000000000',
  dec1 decimal(65,30) unsigned NOT NULL DEFAULT '1.000000000000000000000000000000',
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
  insert into onlineddl_test values (null, 0.0, 0.0);
  insert into onlineddl_test values (null, 2.0, 4.0);
  insert into onlineddl_test values (null, 99999999999999999999999999999999999.000, 6.0);
  update onlineddl_test set dec1=4.5 where dec2=4.0 order by id desc limit 1;
end ;;

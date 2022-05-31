drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  t varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  primary key(id)
) auto_increment=1;

insert into onlineddl_test values (null, '{}');

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
  insert into onlineddl_test values (null, '{}');
  insert into onlineddl_test values (null, '"a"');
  insert into onlineddl_test values (null, '17');
end ;;

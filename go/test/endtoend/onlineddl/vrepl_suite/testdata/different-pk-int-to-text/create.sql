drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  uu varchar(64) not null,
  ts timestamp,
  primary key(id)
) auto_increment=1;

insert into onlineddl_test values (null, uuid(), now());

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
  insert into onlineddl_test values (null, uuid(), now());
  insert into onlineddl_test values (null, uuid(), now());
  insert into onlineddl_test values (null, uuid(), now());
end ;;

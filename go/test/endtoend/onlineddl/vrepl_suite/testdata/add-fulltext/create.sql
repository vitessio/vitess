drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  name1 tinytext not null,
  name2 tinytext not null,
  primary key(id)
) auto_increment=1;

insert into onlineddl_test values (null, "one", "one");

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
  insert into onlineddl_test values (null, "eleven", "eleven");
  insert into onlineddl_test values (null, "thirteen", "thirteen");
  insert into onlineddl_test values (null, "seventeen", "seventeen");
end ;;

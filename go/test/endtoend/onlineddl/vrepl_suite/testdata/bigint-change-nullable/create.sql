drop table if exists onlineddl_test;
create table onlineddl_test (
  id bigint auto_increment,
  val bigint not null,
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
  insert into onlineddl_test values (null, 18446744073709551615);
  insert into onlineddl_test values (null, 18446744073709551614);
  insert into onlineddl_test values (null, 18446744073709551613);
end ;;

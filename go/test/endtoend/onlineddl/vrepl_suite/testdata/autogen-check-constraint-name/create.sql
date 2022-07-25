drop table if exists onlineddl_test;
create table onlineddl_test (
  id bigint not null auto_increment,
  i int not null,
  ts timestamp(6),
  primary key(id),
  constraint `onlineddl_test1_chk_1_031jyu1n2b9j0ap0hxlwuhrvx` CHECK ((`i` >= 0))
) ;

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
  insert into onlineddl_test values (null, 11, now(6));
  insert into onlineddl_test values (null, 13, now(6));
  insert into onlineddl_test values (null, 17, now(6));
  insert into onlineddl_test values (null, 19, now(6));
end ;;

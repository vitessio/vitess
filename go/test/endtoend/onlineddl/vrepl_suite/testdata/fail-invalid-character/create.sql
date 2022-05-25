drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  val varchar(10) charset utf8mb4 not null,
  ts timestamp,
  primary key(id)
) auto_increment=1;

insert into onlineddl_test values (null, 'a', now());
insert into onlineddl_test values (null, 'b', now());

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
  insert into onlineddl_test values (null, 'c', now());
  insert into onlineddl_test values (null, '€', now());
  insert into onlineddl_test values (null, 'd', now());
end ;;

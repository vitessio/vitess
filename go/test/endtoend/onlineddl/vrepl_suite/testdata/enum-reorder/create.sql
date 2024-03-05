drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  i int not null,
  e enum('red', 'green', 'blue') not null,
  primary key(id)
) auto_increment=1;

insert into onlineddl_test values (null, 11, 'red');
insert into onlineddl_test values (null, 13, 'green');
insert into onlineddl_test values (null, 17, 'blue');

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
  insert into onlineddl_test values (null, 211, 'red');
  insert into onlineddl_test values (null, 213, 'green');
  insert into onlineddl_test values (null, 217, 'blue');
end ;;

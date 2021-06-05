drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  a int not null,
  b int not null,
  sum_ab int as (a + b) virtual not null,
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
  insert into onlineddl_test (id, a, b) values (null, 2,3);
  insert into onlineddl_test (id, a, b) values (null, 2,4);
  insert into onlineddl_test (id, a, b) values (null, 2,5);
  insert into onlineddl_test (id, a, b) values (null, 2,6);
  insert into onlineddl_test (id, a, b) values (null, 2,7);
  insert into onlineddl_test (id, a, b) values (null, 2,8);
  insert into onlineddl_test (id, a, b) values (null, 2,9);
  insert into onlineddl_test (id, a, b) values (null, 2,0);
  insert into onlineddl_test (id, a, b) values (null, 2,1);
  insert into onlineddl_test (id, a, b) values (null, 2,2);
end ;;

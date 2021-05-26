drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  i int not null,
  bi bigint not null,
  iu int unsigned not null,
  biu bigint unsigned not null,
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
  insert into onlineddl_test values (null, -2147483647, -9223372036854775807, 4294967295, 18446744073709551615);
  set @last_insert_id := cast(last_insert_id() as signed);
  update onlineddl_test set i=-2147483647+@last_insert_id, bi=-9223372036854775807+@last_insert_id, iu=4294967295-@last_insert_id, biu=18446744073709551615-@last_insert_id where id < @last_insert_id order by id desc limit 1;
end ;;

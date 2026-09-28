drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  i int not null,
  primary key(id)
) auto_increment=1;

insert into onlineddl_test values (null, 0);
insert into onlineddl_test values (null, 1);

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
  insert into onlineddl_test values (null, 0);
  insert into onlineddl_test values (null, 1);
  insert into onlineddl_test values (null, 1);
  set @last_insert_id := last_insert_id();
  update onlineddl_test set i='0' where id = @last_insert_id;
end ;;

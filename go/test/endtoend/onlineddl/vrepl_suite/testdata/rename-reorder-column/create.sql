drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  c1 int not null,
  c2 int not null,
  primary key (id)
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
  insert ignore into onlineddl_test values (1, 11, 23);
  insert ignore into onlineddl_test values (2, 13, 23);
  insert into onlineddl_test values (null, 17, 23);
  set @last_insert_id := last_insert_id();
  update onlineddl_test set c1=c1+@last_insert_id, c2=c2+@last_insert_id where id=@last_insert_id order by id desc limit 1;
  delete from onlineddl_test where id=1;
  delete from onlineddl_test where c1=13; -- id=2
end ;;

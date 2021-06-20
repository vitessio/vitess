drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  c1 int null,
  c2 int not null,
  primary key (id)
) auto_increment=1;

insert into onlineddl_test values (null, null, 17);
insert into onlineddl_test values (null, null, 19);

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
  insert ignore into onlineddl_test values (101, 11, 23);
  insert ignore into onlineddl_test values (102, 13, 23);
  insert into onlineddl_test values (null, 17, 23);
  insert into onlineddl_test values (null, null, 29);
  set @last_insert_id := last_insert_id();
  -- update onlineddl_test set c2=c2+@last_insert_id where id=@last_insert_id order by id desc limit 1;
  delete from onlineddl_test where id=1;
  delete from onlineddl_test where c1=13; -- id=2
end ;;

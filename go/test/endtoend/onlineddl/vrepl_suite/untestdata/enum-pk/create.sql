drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  i int not null,
  e enum('red', 'green', 'blue', 'orange') not null default 'red' collate 'utf8_bin',
  primary key(id, e)
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
  insert into onlineddl_test values (null, 11, 'red');
  set @last_insert_id := last_insert_id();
  insert into onlineddl_test values (@last_insert_id, 11, 'green');
  insert into onlineddl_test values (null, 13, 'green');
  insert into onlineddl_test values (null, 17, 'blue');
  set @last_insert_id := last_insert_id();
  update onlineddl_test set e='orange' where id = @last_insert_id;
  insert into onlineddl_test values (null, 23, null);
  set @last_insert_id := last_insert_id();
  update onlineddl_test set i=i+1, e=null where id = @last_insert_id;
end ;;

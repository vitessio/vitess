drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  i int not null,
  e enum('red', 'green', 'blue', 'orange') null default null collate 'utf8_bin',
  primary key(id)
) auto_increment=1;

insert into onlineddl_test values (null, 7, 'red');

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
  insert into onlineddl_test values (null, 13, 'green');
  insert into onlineddl_test values (null, 17, 'blue');
  set @last_insert_id := last_insert_id();
  update onlineddl_test set e='orange' where id = @last_insert_id;
end ;;

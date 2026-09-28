drop event if exists onlineddl_test;

drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  i int not null,
  primary key(id)
) auto_increment=8;

insert into onlineddl_test values (NULL, 11);
insert into onlineddl_test values (NULL, 13);
insert into onlineddl_test values (NULL, 17);
insert into onlineddl_test values (NULL, 23);
insert into onlineddl_test values (NULL, 29);
insert into onlineddl_test values (NULL, 31);
delete from onlineddl_test where id>=0;

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
  insert into onlineddl_test values (NULL, 37);
  delete from onlineddl_test where id>=0;
end ;;

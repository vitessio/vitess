set session foreign_key_checks=0;
drop table if exists onlineddl_test_child;
drop table if exists onlineddl_test;
drop table if exists onlineddl_test_parent;
set session foreign_key_checks=1;
create table onlineddl_test_parent (
  id int auto_increment,
  ts timestamp,
  primary key(id)
);
create table onlineddl_test (
  id int auto_increment,
  i int not null,
  parent_id int not null,
  primary key(id),
  constraint test_fk foreign key (parent_id) references onlineddl_test_parent (id) on delete no action
) auto_increment=1;

insert into onlineddl_test_parent (id) values (1),(2),(3);

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
  insert into onlineddl_test values (null, 11, 1);
  insert into onlineddl_test values (null, 13, 2);
  insert into onlineddl_test values (null, 17, 3);
end ;;

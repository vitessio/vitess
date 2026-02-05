set session foreign_key_checks=0;
drop table if exists onlineddl_test_child;
drop table if exists onlineddl_test;
drop table if exists onlineddl_test_parent;
set session foreign_key_checks=1;
create table onlineddl_test_parent (
  id int auto_increment,
  primary key(id)
);
create table onlineddl_test (
  id int auto_increment,
  parent_id int null,
  primary key(id),
  constraint test_fk foreign key (parent_id) references onlineddl_test_parent (id) on delete no action
);

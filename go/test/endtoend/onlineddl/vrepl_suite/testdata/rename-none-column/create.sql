drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  c1 int not null,
  primary key (id)
) auto_increment=1;

drop event if exists onlineddl_test;

drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  i int not null,
  ts timestamp,
  primary key(id)
) auto_increment=1;

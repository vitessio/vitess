drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  i int not null,
  primary key(id)
) auto_increment=1;

set session sql_mode='NO_AUTO_VALUE_ON_ZERO';
insert into onlineddl_test values (0, 23);

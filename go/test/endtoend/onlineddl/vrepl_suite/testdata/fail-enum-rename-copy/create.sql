drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  i int not null,
  e enum('red', 'green', 'blue') not null,
  primary key(id)
) auto_increment=1;

insert into onlineddl_test values (null, 11, 'red');
insert into onlineddl_test values (null, 13, 'green');
insert into onlineddl_test values (null, 17, 'blue');

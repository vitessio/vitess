drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  i int not null,
  color varchar(32),
  primary key(id)
) auto_increment=1;

drop event if exists onlineddl_test;

insert into onlineddl_test values (null, 11, 'red');
insert into onlineddl_test values (null, 13, 'green');
insert into onlineddl_test values (null, 17, 'blue');

drop event if exists onlineddl_test;

drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  i int not null,
  primary key(id)
) auto_increment=1;

insert into onlineddl_test values (NULL, 11);
insert into onlineddl_test values (NULL, 13);
insert into onlineddl_test values (NULL, 17);
insert into onlineddl_test values (NULL, 23);

drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  val int not null,
  ts timestamp,
  primary key(id)
) auto_increment=1;

insert into onlineddl_test values (null, 2, now());
insert into onlineddl_test values (null, 3, now());

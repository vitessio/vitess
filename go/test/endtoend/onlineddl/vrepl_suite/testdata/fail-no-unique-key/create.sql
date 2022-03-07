drop table if exists onlineddl_test;
create table onlineddl_test (
  i int not null,
  ts timestamp default current_timestamp,
  dt datetime,
  key i_idx(i)
) auto_increment=1;

drop event if exists onlineddl_test;

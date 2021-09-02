drop table if exists onlineddl_test;
create table onlineddl_test (
  f float,
  i int not null,
  ts timestamp default current_timestamp,
  dt datetime,
  key i_idx(i),
  unique key f_uidx(f)
) auto_increment=1;

drop event if exists onlineddl_test;

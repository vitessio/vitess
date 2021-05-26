drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  t text charset latin1 collate latin1_swedish_ci,
  primary key(id)
) auto_increment=1 charset latin1 collate latin1_swedish_ci;

drop event if exists onlineddl_test;
delimiter ;;
create event onlineddl_test
  on schedule every 1 second
  starts current_timestamp
  ends current_timestamp + interval 60 second
  on completion not preserve
  enable
  do
begin
  insert into onlineddl_test values (null, md5(rand()));
  insert into onlineddl_test values (null, 'átesting');
  insert into onlineddl_test values (null, 'ádelete');
  insert into onlineddl_test values (null, 'testátest');
  update onlineddl_test set t='áupdated' order by id desc limit 1;
  update onlineddl_test set t='áupdated1' where t='áupdated' order by id desc limit 1;
  delete from onlineddl_test where t='ádelete';
end ;;

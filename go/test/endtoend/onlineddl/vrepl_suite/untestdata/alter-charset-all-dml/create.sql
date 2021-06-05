drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  t1 varchar(128)  charset latin1 collate latin1_swedish_ci,
  t2 varchar(128)  charset latin1 collate latin1_swedish_ci,
  tutf8 varchar(128) charset utf8,
  tutf8mb4 varchar(128) charset utf8mb4,
  random_value varchar(128) charset ascii,
  primary key(id)
) auto_increment=1;

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
  insert into onlineddl_test values (null, md5(rand()), md5(rand()), md5(rand()), md5(rand()), md5(rand()));
  insert into onlineddl_test values (null, 'átesting', 'átesting', 'átesting', 'átesting', md5(rand()));
  insert into onlineddl_test values (null, 'átesting_del', 'átesting', 'átesting', 'átesting', md5(rand()));
  insert into onlineddl_test values (null, 'testátest', 'testátest', 'testátest', '🍻😀', md5(rand()));
  update onlineddl_test set t1='átesting2' where t1='átesting' order by id desc limit 1;
  delete from onlineddl_test where t1='átesting_del' order by id desc limit 1;
end ;;

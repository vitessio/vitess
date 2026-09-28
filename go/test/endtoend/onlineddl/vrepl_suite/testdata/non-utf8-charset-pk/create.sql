drop table if exists onlineddl_test;
create table onlineddl_test (
  id varchar(128)  charset latin1 collate latin1_swedish_ci,
  t1 varchar(128)  charset latin1 collate latin1_swedish_ci,
  t2 varchar(128)  charset latin1 collate latin1_swedish_ci,
  tutf8 varchar(128) charset utf8,
  tutf8mb4 varchar(128) charset utf8mb4,
  tlatin1 varchar(128)  charset latin1 collate latin1_swedish_ci,
  primary key(id)
) auto_increment=1;

insert into onlineddl_test values (concat('átesting-', md5(rand())), md5(rand()), md5(rand()), md5(rand()), md5(rand()), md5(rand()));
insert into onlineddl_test values (concat('átesting-', md5(rand())), 'átesting', 'átesting', 'átesting', 'átesting', 'átesting');
insert into onlineddl_test values (concat('átesting-', md5(rand())), 'testátest', 'testátest', 'testátest', '🍻😀', 'átesting');

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
  insert into onlineddl_test values (concat('átesting-', md5(rand())), md5(rand()), md5(rand()), md5(rand()), md5(rand()), md5(rand()));
  insert into onlineddl_test values (concat('átesting-', md5(rand())), 'átesting-binlog', 'átesting-binlog', 'átesting-binlog', 'átesting-binlog', 'átesting-binlog');
  insert into onlineddl_test values (concat('átesting-', md5(rand())), 'testátest-binlog', 'testátest-binlog', 'testátest-binlog', '🍻😀', 'átesting-binlog');
  insert into onlineddl_test values (concat('átesting-', md5(rand())), 'átesting-bnull', 'átesting-bnull', 'átesting-bnull', null, null);
end ;;

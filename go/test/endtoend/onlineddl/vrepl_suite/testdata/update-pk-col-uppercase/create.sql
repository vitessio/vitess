drop table if exists onlineddl_test;
create table onlineddl_test (
  Id varchar(64) NOT NULL,
  val varchar(64) DEFAULT NULL,
  dt datetime DEFAULT NULL,
  primary key (Id)
) auto_increment=1;

insert ignore into onlineddl_test values (md5(rand()), md5(rand()), now());
insert ignore into onlineddl_test values (md5(rand()), md5(rand()), now());
insert ignore into onlineddl_test values (md5(rand()), md5(rand()), now());
insert ignore into onlineddl_test values (md5(rand()), md5(rand()), now());
insert ignore into onlineddl_test values (md5(rand()), md5(rand()), now());
insert ignore into onlineddl_test values (md5(rand()), md5(rand()), now());
insert ignore into onlineddl_test values (md5(rand()), md5(rand()), now());
insert ignore into onlineddl_test values (md5(rand()), md5(rand()), now());

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
  update onlineddl_test set Id=md5(rand()) order by rand() limit 1;
end ;;

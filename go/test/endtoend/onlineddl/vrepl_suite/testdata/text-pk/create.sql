drop table if exists onlineddl_test;
create table onlineddl_test (
  id varchar(64) not null,
  val int default null,
  ts timestamp,
  primary key(id)
) auto_increment=1;

insert into onlineddl_test values (sha1(rand()), 2, now());
insert into onlineddl_test values (sha1(rand()), 3, now());

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
  insert into onlineddl_test values (sha1(rand()), 11, now());
end ;;

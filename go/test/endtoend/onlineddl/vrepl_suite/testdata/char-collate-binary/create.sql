drop table if exists onlineddl_test;
create table onlineddl_test (
  id bigint auto_increment,
  country_code char(3) collate utf8mb4_bin,
  primary key(id)
) auto_increment=1;

insert into onlineddl_test values (null, 'ABC');
insert into onlineddl_test values (null, 'DEF');
insert into onlineddl_test values (null, 'GHI');

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
  insert into onlineddl_test values (null, 'jkl');
  insert into onlineddl_test values (null, 'MNO');
end ;;

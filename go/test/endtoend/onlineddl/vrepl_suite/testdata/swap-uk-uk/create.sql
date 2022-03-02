drop table if exists onlineddl_test;
create table onlineddl_test (
  id bigint not null,
  i int not null,
  ts timestamp(6) not null,
  unique key id_uidx(id),
  unique key its_uidx(i, ts)
) ;

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
  insert into onlineddl_test values ((unix_timestamp() << 2) + 0, 11, now(6));
  insert into onlineddl_test values ((unix_timestamp() << 2) + 1, 13, now(6));
  insert into onlineddl_test values ((unix_timestamp() << 2) + 2, 17, now(6));
  insert into onlineddl_test values ((unix_timestamp() << 2) + 3, 19, now(6));
end ;;

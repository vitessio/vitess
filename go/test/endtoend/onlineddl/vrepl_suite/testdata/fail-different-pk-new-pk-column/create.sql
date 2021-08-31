drop table if exists onlineddl_test;
create table onlineddl_test (
  uu varchar(64) not null,
  ts timestamp,
  primary key(uu)
);

insert into onlineddl_test values (uuid(), now());

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
  insert into onlineddl_test values (uuid(), now());
  insert into onlineddl_test values (uuid(), now());

  set @uu := uuid();
  insert into onlineddl_test values (@uu, now());
  update onlineddl_test set ts=now() + interval 1 hour where uu=@uu;
end ;;

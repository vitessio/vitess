drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  bin_data varbinary(256) not null,
  primary key(id)
) auto_increment=1;

-- Insert rows with binary data containing bytes 0x80-0xFF.
-- These bytes are invalid standalone UTF-8. When VReplication copies these rows,
-- the bulk INSERT query is built by encodeBytesSQLBytes2 (byte iteration, WriteByte),
-- preserving raw high bytes. If the error message is truncated and then re-encoded
-- by EncodeStringSQL (rune iteration, WriteRune), each invalid byte expands from
-- 1 byte to 3 bytes (U+FFFD), causing the stored message to exceed varbinary(1000).
insert into onlineddl_test (bin_data) values
  (UNHEX(REPEAT('FF80FE90FD82FC83', 16))),
  (UNHEX(REPEAT('FF80FE90FD82FC83', 16))),
  (UNHEX(REPEAT('FF80FE90FD82FC83', 16))),
  (UNHEX(REPEAT('FF80FE90FD82FC83', 16))),
  (UNHEX(REPEAT('FF80FE90FD82FC83', 16))),
  (UNHEX(REPEAT('FF80FE90FD82FC83', 16))),
  (UNHEX(REPEAT('FF80FE90FD82FC83', 16))),
  (UNHEX(REPEAT('FF80FE90FD82FC83', 16))),
  (UNHEX(REPEAT('FF80FE90FD82FC83', 16))),
  (UNHEX(REPEAT('FF80FE90FD82FC83', 16))),
  (UNHEX(REPEAT('FF80FE90FD82FC83', 16))),
  (UNHEX(REPEAT('FF80FE90FD82FC83', 16))),
  (UNHEX(REPEAT('FF80FE90FD82FC83', 16))),
  (UNHEX(REPEAT('FF80FE90FD82FC83', 16))),
  (UNHEX(REPEAT('FF80FE90FD82FC83', 16))),
  (UNHEX(REPEAT('FF80FE90FD82FC83', 16))),
  (UNHEX(REPEAT('FF80FE90FD82FC83', 16))),
  (UNHEX(REPEAT('FF80FE90FD82FC83', 16))),
  (UNHEX(REPEAT('FF80FE90FD82FC83', 16))),
  (UNHEX(REPEAT('FF80FE90FD82FC83', 16)));

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
  insert into onlineddl_test values (null, UNHEX(REPEAT('FF80FE90FD82FC83', 16)));
end ;;

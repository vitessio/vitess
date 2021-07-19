drop table if exists onlineddl_test;
create table onlineddl_test (
  id binary(16) NOT NULL,
  info varchar(255) COLLATE utf8_unicode_ci NOT NULL,
  data binary(8) NOT NULL,
  primary key (id),
  unique key info_uidx (info)
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
  replace into onlineddl_test (id, info, data) values (X'12ffffffffffffffffffffffffffff00', 'item 1a', X'12ffffffffffffff');
  replace into onlineddl_test (id, info, data) values (X'34ffffffffffffffffffffffffffffff', 'item 3a', X'34ffffffffffffff');
  replace into onlineddl_test (id, info, data) values (X'90ffffffffffffffffffffffffffffff', 'item 9a', X'90ffffffffffff00');

  DELETE FROM onlineddl_test WHERE id = X'11ffffffffffffffffffffffffffff00';
  UPDATE onlineddl_test SET info = 'item 2++' WHERE id = X'22ffffffffffffffffffffffffffff00';
  UPDATE onlineddl_test SET info = 'item 3++', data = X'33ffffffffffff00' WHERE id = X'33ffffffffffffffffffffffffffffff';
  DELETE FROM onlineddl_test WHERE id = X'44ffffffffffffffffffffffffffffff';
  UPDATE onlineddl_test SET info = 'item 5++', data = X'55ffffffffffffee'  WHERE id = X'55ffffffffffffffffffffffffffffff';
  INSERT INTO onlineddl_test (id, info, data) VALUES (X'66ffffffffffffffffffffffffffff00', 'item 6', X'66ffffffffffffff');
  INSERT INTO onlineddl_test (id, info, data) VALUES (X'77ffffffffffffffffffffffffffffff', 'item 7', X'77ffffffffffff00');
  INSERT INTO onlineddl_test (id, info, data) VALUES (X'88ffffffffffffffffffffffffffffff', 'item 8', X'88ffffffffffffff');
end ;;

INSERT INTO onlineddl_test (id, info, data) VALUES
 (X'11ffffffffffffffffffffffffffff00', 'item 1', X'11ffffffffffffff'), -- id ends in 00
 (X'22ffffffffffffffffffffffffffff00', 'item 2', X'22ffffffffffffff'), -- id ends in 00
 (X'33ffffffffffffffffffffffffffffff', 'item 3', X'33ffffffffffffff'),
 (X'44ffffffffffffffffffffffffffffff', 'item 4', X'44ffffffffffffff'),
 (X'55ffffffffffffffffffffffffffffff', 'item 5', X'55ffffffffffffff'),
 (X'99ffffffffffffffffffffffffffffff', 'item 9', X'99ffffffffffff00'); -- data ends in 00

drop table if exists onlineddl_test;
create table onlineddl_test (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  column1 int(11) NOT NULL,
  column2 smallint(5) unsigned NOT NULL,
  column3 mediumint(8) unsigned NOT NULL,
  column4 tinyint(3) unsigned NOT NULL,
  column5 int(11) NOT NULL,
  column6 int(11) NOT NULL,
  PRIMARY KEY (id),
  KEY c12_ix (column1, column2)
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
  -- mediumint maxvalue: 16777215 (unsigned), 8388607 (signed)
  insert into onlineddl_test values (NULL, 13382498, 536,  8388607, 3, 1483892217, 1483892218);
  insert into onlineddl_test values (NULL, 13382498, 536,  8388607, 250, 1483892217, 1483892218);
  insert into onlineddl_test values (NULL, 13382498, 536, 10000000, 3, 1483892217, 1483892218);
end ;;

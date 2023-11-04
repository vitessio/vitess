create table test_table (
  id bigint,
  name varchar(64),
  age SMALLINT,
  percent DECIMAL(5,2),
  datetime_col DATETIME,
  timestamp_col TIMESTAMP,
  date_col DATE,
  time_col TIME,
  primary key (id)
) Engine=InnoDB;


CREATE TABLE benchmark_table (
  id bigint NOT NULL,
  - data varbinary(128),
  + data varchar(128),
  PRIMARY KEY (id)
); 
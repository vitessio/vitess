CREATE TABLE integration_test (
  test_id bigint NOT NULL,
  - test_data varbinary(128),
  + test_data varchar(128),
  PRIMARY KEY (test_id)
);

INSERT INTO integration_test VALUES
  - (1, _binary 'test_value_1'),
  + (1, 'test_value_1'),
  - (2, _binary 'test_value_2'),
  + (2, 'test_value_2'); 
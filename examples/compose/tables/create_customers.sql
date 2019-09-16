CREATE TABLE customers (
  id BIGINT UNSIGNED,
  time_created_ns BIGINT UNSIGNED,
  name VARCHAR(255),
  balance INT,
  PRIMARY KEY (id)
) ENGINE=InnoDB;
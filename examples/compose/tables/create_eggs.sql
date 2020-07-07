CREATE TABLE eggs (
  id BIGINT UNSIGNED,
  time_created_ns BIGINT UNSIGNED,
  species VARCHAR(255),
  PRIMARY KEY (id)
) ENGINE=InnoDB;
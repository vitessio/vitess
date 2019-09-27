CREATE TABLE messages (
  page BIGINT(20) UNSIGNED,
  time_created_ns timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  message VARCHAR(1000),
  PRIMARY KEY (page, time_created_ns)
) ENGINE=InnoDB;
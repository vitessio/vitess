CREATE TABLE tokens (
  page BIGINT(20) UNSIGNED,
  time_created_ns timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  token VARCHAR(255) DEFAULT NULL,
  PRIMARY KEY (page, time_created_ns)
) ENGINE=InnoDB;
CREATE TABLE dinosaurs (
  id BIGINT UNSIGNED,
  time_created_ns BIGINT UNSIGNED,
  name VARCHAR(255),
  PRIMARY KEY (id)
) ENGINE=InnoDB;
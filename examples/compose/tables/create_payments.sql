CREATE TABLE payments (
  id BIGINT UNSIGNED,
  time_created_ns BIGINT UNSIGNED,
  customer_id BIGINT UNSIGNED,
  misc_id BIGINT UNSIGNED,
  external_id VARCHAR(255) DEFAULT NULL,
  value INT,
  PRIMARY KEY (id),
  KEY `customer_id_idx` (`customer_id`),
  KEY `misc_id_idx` (`misc_id`),
  KEY `external_id_idx` (`external_id`)
) ENGINE=InnoDB;
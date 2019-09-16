CREATE TABLE payments_external_id_lookup (
  id BIGINT NOT NULL AUTO_INCREMENT,
  customer_id BIGINT UNSIGNED,
  external_id VARCHAR(255) DEFAULT NULL,
  PRIMARY KEY (id),
  KEY `external_id_idx` (`external_id`)
) ENGINE=InnoDB;
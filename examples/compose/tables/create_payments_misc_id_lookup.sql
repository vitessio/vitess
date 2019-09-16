CREATE TABLE payments_misc_id_lookup (
  id BIGINT NOT NULL AUTO_INCREMENT,
  customer_id BIGINT UNSIGNED,
  misc_id BIGINT UNSIGNED,
  PRIMARY KEY (id),
  KEY `misc_id_idx` (`misc_id`)
) ENGINE=InnoDB;
CREATE TABLE tokens_token_lookup (
  id BIGINT NOT NULL AUTO_INCREMENT,
  page BIGINT UNSIGNED,
  token VARCHAR(255) DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY idx_token_page (`token`, `page`)
) ENGINE=InnoDB;

CREATE TABLE messages_message_lookup (
  id BIGINT NOT NULL AUTO_INCREMENT,
  page BIGINT UNSIGNED,
  message VARCHAR(1000),
  PRIMARY KEY (id),
  UNIQUE KEY idx_message_page (`message`, `page`)
) ENGINE=InnoDB;


CREATE TABLE test_table (
  `id` BIGINT(20) NOT NULL,
  `msg` VARCHAR(64),
  `keyspace_id` BIGINT(20) UNSIGNED NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB

CREATE TABLE `t1` (
  `id` bigint unsigned NOT NULL,
  `unique_num` int NOT NULL,
  PRIMARY KEY(`id`),
  UNIQUE KEY `index_t1_on_unique_num` (`unique_num`)
) ENGINE=InnoDB;

CREATE TABLE `t2_lookup_id_keyspace_idx` (
  `lookup_id` VARBINARY(128) NOT NULL,
  `keyspace_id` VARBINARY(128) NOT NULL,
  PRIMARY KEY(`lookup_id`)
) ENGINE=InnoDB;

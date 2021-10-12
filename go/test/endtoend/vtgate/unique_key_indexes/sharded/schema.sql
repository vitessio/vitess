CREATE TABLE `t2` (
  `sharding_id` bigint unsigned NOT NULL,
  `lookup_id` bigint unsigned NOT NULL,
  `unique_num` int NOT NULL,
  UNIQUE KEY `index_t2_on_unique_num` (`sharding_id`, `unique_num`)
) ENGINE=InnoDB;

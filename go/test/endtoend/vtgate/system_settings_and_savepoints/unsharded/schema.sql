CREATE TABLE `t1_id_seq` (
  `id` bigint,
  `next_id` bigint,
  `cache` bigint,
  PRIMARY KEY(`id`)
) COMMENT 'vitess_sequence';

INSERT INTO `t1_id_seq` (id, next_id, cache) VALUES (0, 1, 1);

CREATE TABLE `t1_id_keyspace_idx` (
  `id` VARBINARY(128) NOT NULL,
  `keyspace_id` VARBINARY(128) NOT NULL,
  PRIMARY KEY(`id`)
);

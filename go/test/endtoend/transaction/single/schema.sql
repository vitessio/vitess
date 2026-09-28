CREATE TABLE `txn_unique_constraints` (
    `id`                varchar(100) NOT NULL ,
    `txn_id`            varchar(50) NOT NULL,
    `unique_constraint` varchar(100) NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `idx_txn_unique_constraint` (`unique_constraint`),
    KEY `idx_txn_unique_constraints_txn_id` (`txn_id`)
) ENGINE=InnoDB;

CREATE TABLE uniqueConstraint_vdx(
    `unique_constraint` VARCHAR(50) NOT NULL,
    `keyspace_id`       VARBINARY(50) NOT NULL,
    PRIMARY KEY(unique_constraint)
) ENGINE=InnoDB;

CREATE TABLE `t1` (
    `id` bigint(20) NOT NULL,
    `txn_id` varchar(50) DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `t1_id_vdx` (
    `id` bigint(20) NOT NULL,
    `keyspace_id` varbinary(50) NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `t2` (
    `id` bigint(20) NOT NULL,
    `txn_id` varchar(50) DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `t2_id_vdx` (
    `id` bigint(20) NOT NULL,
    `keyspace_id` varbinary(50) NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
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
-- The items in the below lists correspond to the following column names:
-- PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, PKCOLUMN_NAME, FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, FKCOLUMN_NAME
-- KEY_SEQ, UPDATE_RULE, DELETE_RULE, FK_NAME, PK_NAME, DEFERRABILITY

-- Four of those columns are integers. Three other columns should always be null.
-- The first group has 3 integers, i.e. `1, 3, 3`. This represents KEY_SEQ, UPDATE_RULE, and DELETE_RULE, in order.
-- KEY_SEQ should increment for each column in a key, so 1 for the first, 2 for the second, etc, within a single constraint.
-- UPDATE_RULE and DELETE_RULE correspond to one of:
--   DatabaseMetaData.importedKeyCascade (0), importedKeyRestrict (1), importedKeySetNull (2), or importedKeyNoAction (3)
-- The fourth integer is the last value in the list. This should be DatabaseMetaData.importedKeyNotDeferrable (7) for all cases.

-- name: single fk constraint no update/delete references
-- expected: [[test, null, fTable, id, test, null, testA, fIdOne, 1, 3, 3, fk_testA, null, 7]]
CREATE TABLE `testA` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `fIdOne` bigint(20) unsigned NOT NULL,
  `fIdTwo` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_testA` (`fIdOne`,`fIdTwo`),
  CONSTRAINT `fk_testA` FOREIGN KEY (`fIdOne`) REFERENCES `fTable` (`id`)
) ENGINE=InnoDB

-- name: single fk reference different schema/catalog
-- expected: [[otherCatalog, null, fTable, id, test, null, testA, fIdOne, 1, 3, 3, fk_testA, null, 7]]
CREATE TABLE `testA` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `fIdOne` bigint(20) unsigned NOT NULL,
  `fIdTwo` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_testA` (`fIdOne`,`fIdTwo`),
  CONSTRAINT `fk_testA` FOREIGN KEY (`fIdOne`) REFERENCES `otherCatalog`.`fTable` (`id`)
) ENGINE=InnoDB

-- name: single fk constraint delete cascade
-- expected: [[test, null, fTable, id, test, null, testA, fIdOne, 1, 3, 0, fk_testA, null, 7]]
CREATE TABLE `testA` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `fIdOne` bigint(20) unsigned NOT NULL,
  `fIdTwo` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_testA` (`fIdOne`,`fIdTwo`),
  CONSTRAINT `fk_testA` FOREIGN KEY (`fIdOne`) REFERENCES `fTable` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB

-- name: single fk constraint on update restrict
-- expected: [[test, null, fTable, id, test, null, testA, fIdOne, 1, 1, 3, fk_testA, null, 7]]
CREATE TABLE `testA` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `fIdOne` bigint(20) unsigned NOT NULL,
  `fIdTwo` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_testA` (`fIdOne`,`fIdTwo`),
  CONSTRAINT `fk_testA` FOREIGN KEY (`fIdOne`) REFERENCES `fTable` (`id`) ON UPDATE RESTRICT
) ENGINE=InnoDB

-- name: single fk constraint update and delete
-- expected: [[test, null, fTable, id, test, null, testA, fIdOne, 1, 2, 0, fk_testA, null, 7]]
CREATE TABLE `testA` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `fIdOne` bigint(20) unsigned NOT NULL,
  `fIdTwo` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_testA` (`fIdOne`,`fIdTwo`),
  CONSTRAINT `fk_testA` FOREIGN KEY (`fIdOne`) REFERENCES `fTable` (`id`) ON DELETE CASCADE ON UPDATE SET NULL
) ENGINE=InnoDB

-- name: no constraint name
-- expected: [[test, null, fTable, id, test, null, testA, fIdOne, 1, 3, 3, not_available, null, 7]]
CREATE TABLE `testA` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `fIdOne` bigint(20) unsigned NOT NULL,
  `fIdTwo` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_testA` (`fIdOne`,`fIdTwo`),
  FOREIGN KEY (`fIdOne`) REFERENCES `fTable` (`id`)
) ENGINE=InnoDB

-- name: multiple fk constraints delete cascade
-- expected: [[test, null, fTableOne, id, test, null, testA, fIdOne, 1, 3, 0, fk_testA_fTableTwo, null, 7], [test, null, fTableTwo, id, test, null, testA, fIdTwo, 1, 3, 0, fk_testA_fTableOne, null, 7]]
CREATE TABLE `testA` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `fIdOne` bigint(20) unsigned NOT NULL,
  `fIdTwo` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_testA` (`fIdOne`,`fIdTwo`),
  CONSTRAINT `fk_testA_fTableTwo` FOREIGN KEY (`fIdOne`) REFERENCES `fTableOne` (`id`) ON DELETE CASCADE
  CONSTRAINT `fk_testA_fTableOne` FOREIGN KEY (`fIdTwo`) REFERENCES `fTableTwo` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB

-- name: multiple fk constraints and multiple columns in keys delete cascade
-- expected: [[test, null, fTableOne, id, test, null, testA, fIdOne, 1, 3, 0, fk_testA_fTableOne, null, 7], [test, null, fTableOne, other, test, null, testA, fIdTwo, 2, 3, 0, fk_testA_fTableOne, null, 7], [test, null, fTableTwo, id, test, null, testA, fIdThree, 1, 3, 0, fk_testA_fTableTwo, null, 7]]
CREATE TABLE `testA` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `fIdOne` bigint(20) unsigned NOT NULL,
  `fIdTwo` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_testA` (`fIdOne`,`fIdTwo`),
  CONSTRAINT `fk_testA_fTableOne` FOREIGN KEY (`fIdOne`, `fIdTwo`) REFERENCES `fTableOne` (`id`, `other`) ON DELETE CASCADE
  CONSTRAINT `fk_testA_fTableTwo` FOREIGN KEY (`fIdThree`) REFERENCES `fTableTwo` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB

-- name: single fk constraint, multiple keys
-- expected: [[test, null, fTable, id, test, null, testA, fIdOne, 1, 3, 3, fk_testA, null, 7], [test, null, fTable, other, test, null, testA, fIdTwo, 2, 3, 3, fk_testA, null, 7]]
CREATE TABLE `testA` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `fIdOne` bigint(20) unsigned NOT NULL,
  `fIdTwo` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_testA` (`fIdOne`,`fIdTwo`),
  CONSTRAINT `fk_testA` FOREIGN KEY (`fIdOne`, `fIdTwo`) REFERENCES `fTable` (`id`, `other`)
) ENGINE=InnoDB

-- name: with index name
-- expected: [[test, null, fTable, id, test, null, testA, fIdOne, 1, 3, 3, fk_testA, null, 7]]
CREATE TABLE `testA` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `fIdOne` bigint(20) unsigned NOT NULL,
  `fIdTwo` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_testA` (`fIdOne`,`fIdTwo`),
  CONSTRAINT `fk_testA` FOREIGN KEY `idx_fk_testA` (`fIdOne`) REFERENCES `fTable` (`id`)
) ENGINE=InnoDB

-- name: double quote constraint name
-- expected: [[test, null, fTable, id, test, null, testA, fIdOne, 1, 3, 3, fk_testA, null, 7]]
CREATE TABLE `testA` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `fIdOne` bigint(20) unsigned NOT NULL,
  `fIdTwo` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_testA` (`fIdOne`,`fIdTwo`),
  CONSTRAINT "fk_testA" FOREIGN KEY (fIdOne) REFERENCES fTable (id)
) ENGINE=InnoDB

-- name: no quotes on constraint name
-- expected: []
-- constraint is required to be quoted, so no match found
CREATE TABLE `testA` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `fIdOne` bigint(20) unsigned NOT NULL,
  `fIdTwo` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_testA` (`fIdOne`,`fIdTwo`),
  CONSTRAINT fk_testA FOREIGN KEY (fIdOne) REFERENCES fTable (id)
) ENGINE=InnoDB

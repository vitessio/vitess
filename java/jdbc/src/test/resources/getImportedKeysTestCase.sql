CREATE TABLE `testA` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `fIdOne` bigint(20) unsigned NOT NULL,
  `fIdTwo` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_testA` (`fIdOne`,`fIdTwo`),
  CONSTRAINT `fk_testA` FOREIGN KEY (`fIdOne`) REFERENCES `fTable` (`id`)
) ENGINE=InnoDB
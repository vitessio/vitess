CREATE DATABASE IF NOT EXISTS commerce;
USE commerce;
DROP TABLE IF EXISTS users;
CREATE TABLE users (
   device_id BIGINT,
   first_name VARCHAR(50),
   last_name VARCHAR(50),
   telephone BIGINT,
   gender VARCHAR(16),
   reference_id INT,
   confidence INT,
   coverage INT,
   refstart DATETIME,
   refstop DATETIME,
   qrystart DATETIME,
   qrystop DATETIME);

LOAD DATA LOCAL INFILE '/docker-entrypoint-initdb.d/dataset.csv' INTO TABLE users FIELDS TERMINATED BY ',';

ALTER TABLE users ADD id INT NOT NULL AUTO_INCREMENT PRIMARY KEY;
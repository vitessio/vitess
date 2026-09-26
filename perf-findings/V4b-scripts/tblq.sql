SET profiling=1;
SELECT table_name, table_type, data_length, table_rows FROM information_schema.tables WHERE table_schema = 'vt_many' AND table_type = 'BASE TABLE';
SELECT table_name, table_type, data_length, table_rows FROM information_schema.tables WHERE table_schema = 'vt_many' AND table_type = 'BASE TABLE';
SELECT table_name, table_type, data_length, table_rows FROM information_schema.tables WHERE table_schema = 'vt_many' AND table_type = 'BASE TABLE' AND table_name IN ('t1250');
SHOW CREATE DATABASE IF NOT EXISTS `vt_many`;
SHOW PROFILES;

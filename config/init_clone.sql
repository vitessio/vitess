SET @original_super_read_only=IF(@@global.super_read_only=1, 'ON', 'OFF');
SET GLOBAL super_read_only='OFF';

# Changes during the init db should not make it to the binlog.
# They could potentially create errant transactions on replicas.
SET sql_log_bin = 0;

# User for MySQL CLONE operations.
# BACKUP_ADMIN is required on the donor for clone operations.
/*!80017 CREATE USER IF NOT EXISTS 'vt_clone'@'%' */;
/*!80017 GRANT BACKUP_ADMIN ON *.* TO 'vt_clone'@'%' */;

SET GLOBAL super_read_only=IFNULL(@original_super_read_only, 'ON');
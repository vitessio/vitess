# User for MySQL CLONE operations.
# BACKUP_ADMIN is required on the donor for clone operations.
/*!80017 CREATE USER IF NOT EXISTS 'vt_clone'@'%' */;
/*!80017 GRANT BACKUP_ADMIN ON *.* TO 'vt_clone'@'%' */;

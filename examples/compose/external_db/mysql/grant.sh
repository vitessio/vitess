
echo '**********GRANTING PRIVILEGES START*******************'
echo ${mysql[@]}
# PURGE BINARY LOGS BEFORE DATE(NOW());
mysql --protocol=socket -uroot -hlocalhost --socket=/var/run/mysqld/mysqld.sock -p$MYSQL_ROOT_PASSWORD -e \
"GRANT ALL PRIVILEGES ON *.* TO '$MYSQL_USER'@'%' IDENTIFIED BY '$MYSQL_PASSWORD'; FLUSH PRIVILEGES;"
echo '*************GRANTING PRIVILEGES END****************'
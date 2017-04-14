#!/bin/bash
set -e

# Download and extract MariaDB 10.0 packages.

# As of 07/2015, this mirror is the fastest for the Travis CI workers.
DEB_PACKAGES="
http://nyc2.mirrors.digitalocean.com/mariadb/repo/10.0/ubuntu/pool/main/m/mariadb-10.0/mysql-common_10.0.30+maria-1~precise_all.deb
http://nyc2.mirrors.digitalocean.com/mariadb/repo/10.0/ubuntu/pool/main/m/mariadb-10.0/mariadb-common_10.0.30+maria-1~precise_all.deb
http://nyc2.mirrors.digitalocean.com/mariadb/repo/10.0/ubuntu/pool/main/m/mariadb-10.0/libmysqlclient18_10.0.30+maria-1~precise_amd64.deb
http://nyc2.mirrors.digitalocean.com/mariadb/repo/10.0/ubuntu/pool/main/m/mariadb-10.0/libmariadbclient18_10.0.30+maria-1~precise_amd64.deb
http://nyc2.mirrors.digitalocean.com/mariadb/repo/10.0/ubuntu/pool/main/m/mariadb-10.0/libmariadbclient-dev_10.0.30+maria-1~precise_amd64.deb
http://nyc2.mirrors.digitalocean.com/mariadb/repo/10.0/ubuntu/pool/main/m/mariadb-10.0/mariadb-client-core-10.0_10.0.30+maria-1~precise_amd64.deb
http://nyc2.mirrors.digitalocean.com/mariadb/repo/10.0/ubuntu/pool/main/m/mariadb-10.0/mariadb-client-10.0_10.0.30+maria-1~precise_amd64.deb
http://nyc2.mirrors.digitalocean.com/mariadb/repo/10.0/ubuntu/pool/main/m/mariadb-10.0/mariadb-server-core-10.0_10.0.30+maria-1~precise_amd64.deb
http://nyc2.mirrors.digitalocean.com/mariadb/repo/10.0/ubuntu/pool/main/m/mariadb-10.0/mariadb-server-10.0_10.0.30+maria-1~precise_amd64.deb
"

mkdir -p $MYSQL_ROOT
if [ -d "$MYSQL_ROOT/usr" ]; then
  echo "skipping downloading and extracting MariaDB packages because it seems they have been cached by Travis CI."
  echo "Delete the cache through the Travis CI Settings page on the webinterface if you want to enforce this step."
  exit 0
fi

for deb in $DEB_PACKAGES; do
  wget $deb
done

for deb in *.deb; do
  dpkg -x $deb $MYSQL_ROOT
done

# Uncomment to debug any issues and inspect which files are available.
# ls -alR $MYSQL_ROOT

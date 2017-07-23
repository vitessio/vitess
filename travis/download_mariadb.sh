#!/bin/bash

# Copyright 2017 Google Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -e

# Download and extract MariaDB 10.0 packages.

# As of 07/2015, this mirror is the fastest for the Travis CI workers.
DEB_PACKAGES="
http://nyc2.mirrors.digitalocean.com/mariadb/repo/10.0/ubuntu/pool/main/m/mariadb-10.0/mysql-common_10.0.31+maria-1~precise_all.deb
http://nyc2.mirrors.digitalocean.com/mariadb/repo/10.0/ubuntu/pool/main/m/mariadb-10.0/mariadb-common_10.0.31+maria-1~precise_all.deb
http://nyc2.mirrors.digitalocean.com/mariadb/repo/10.0/ubuntu/pool/main/m/mariadb-10.0/libmysqlclient18_10.0.31+maria-1~precise_amd64.deb
http://nyc2.mirrors.digitalocean.com/mariadb/repo/10.0/ubuntu/pool/main/m/mariadb-10.0/libmariadbclient18_10.0.31+maria-1~precise_amd64.deb
http://nyc2.mirrors.digitalocean.com/mariadb/repo/10.0/ubuntu/pool/main/m/mariadb-10.0/libmariadbclient-dev_10.0.31+maria-1~precise_amd64.deb
http://nyc2.mirrors.digitalocean.com/mariadb/repo/10.0/ubuntu/pool/main/m/mariadb-10.0/mariadb-client-core-10.0_10.0.31+maria-1~precise_amd64.deb
http://nyc2.mirrors.digitalocean.com/mariadb/repo/10.0/ubuntu/pool/main/m/mariadb-10.0/mariadb-client-10.0_10.0.31+maria-1~precise_amd64.deb
http://nyc2.mirrors.digitalocean.com/mariadb/repo/10.0/ubuntu/pool/main/m/mariadb-10.0/mariadb-server-core-10.0_10.0.31+maria-1~precise_amd64.deb
http://nyc2.mirrors.digitalocean.com/mariadb/repo/10.0/ubuntu/pool/main/m/mariadb-10.0/mariadb-server-10.0_10.0.31+maria-1~precise_amd64.deb
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

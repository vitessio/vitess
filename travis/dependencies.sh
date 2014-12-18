#!/bin/bash
set -e

# Add MariaDB repository
sudo apt-get install python-software-properties
sudo apt-key adv --recv-keys --keyserver hkp://keyserver.ubuntu.com:80 0xcbcb082a1bb943db
sudo add-apt-repository 'deb http://ftp.utexas.edu/mariadb/repo/10.0/ubuntu precise main'
sudo apt-get update

# Remove pre-installed mysql
sudo apt-get purge mysql* mariadb*

# MariaDB
sudo apt-get -f install \
libmysqlclient18=10.0.15+maria-1~precise \
libmariadbclient18=10.0.15+maria-1~precise \
libmariadbclient-dev=10.0.15+maria-1~precise \
mariadb-client-10.0=10.0.15+maria-1~precise \
mariadb-client-core-10.0=10.0.15+maria-1~precise \
mariadb-server-10.0=10.0.15+maria-1~precise \
mariadb-server-core-10.0=10.0.15+maria-1~precise

# Other dependencies
sudo apt-get install time automake libtool memcached python-dev python-mysqldb libssl-dev g++ mercurial git pkg-config bison bc

# Java dependencies
wget https://dl.bintray.com/sbt/debian/sbt-0.13.6.deb
sudo dpkg -i sbt-0.13.6.deb
sudo apt-get -y install protobuf-compiler maven
echo "Y" | sudo apt-get -y install rsyslog
sudo service rsyslog restart
set +e

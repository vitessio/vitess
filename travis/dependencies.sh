#!/bin/bash
set -e

# Add MariaDB repository
sudo apt-get install python-software-properties
sudo apt-key adv --recv-keys --keyserver hkp://keyserver.ubuntu.com:80 0xcbcb082a1bb943db
sudo add-apt-repository 'deb http://ftp.utexas.edu/mariadb/repo/10.0/ubuntu precise main'

# Add New Relic repo
sudo sh -c 'echo deb http://apt.newrelic.com/debian/ newrelic non-free >> /etc/apt/sources.list.d/newrelic.list'
wget -O- https://download.newrelic.com/548C16BF.gpg | sudo apt-key add -

sudo apt-get update

# Install New relic to monitor perf metrics
# Travis will not set license key for forked pull
# requests, so skip the install.
if ! [ -z "$$NEWRELIC_LICENSE_KEY" ]; then
  sudo apt-get install newrelic-sysmond
  sudo nrsysmond-config --set license_key=$NEWRELIC_LICENSE_KEY
  sudo /etc/init.d/newrelic-sysmond start
fi

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

#!/usr/bin/env bash
#
# See http://vitess.io/getting-started/local-instance.html#manual-build
# for more info
#
set -ex

TMP_DIR="$(mktemp -d)"
SEED_FILE='/root/.provisioning_done'

if [ -f $SEED_FILE ];
then
    printf "\nVM provisioning already completed\n"
    exit 0
fi

# Install pre-requisites
add-apt-repository -y ppa:openjdk-r/ppa
apt-get update
apt-get install -y make \
                   automake \
                   libtool \
                   python-dev \
                   python-virtualenv \
                   python-mysqldb \
                   libssl-dev \
                   g++ \
                   mercurial \
                   git \
                   pkg-config \
                   bison \
                   curl \
                   openjdk-7-jre \
                   zip \
                   unzip

# Install golang
GO_VER='1.11.1'
GO_DOWNLOAD_URL='https://storage.googleapis.com/golang'
GO_FILENAME="go${GO_VER}.linux-amd64.tar.gz"
wget "${GO_DOWNLOAD_URL}/${GO_FILENAME}" -O "${TMP_DIR}/${GO_FILENAME}"
tar xzf "${TMP_DIR}/${GO_FILENAME}" -C "/usr/local"

# Install MySQL Percona 5.7 (via APT)
PERCONA_APT="https://repo.percona.com/apt/percona-release_0.1-4.$(lsb_release -sc)_all.deb"
PERCONA_APT_FILENAME='percona-server.tar'
wget "${PERCONA_APT}" -O "${TMP_DIR}/${PERCONA_APT_FILENAME}"
dpkg -i "${TMP_DIR}/${PERCONA_APT_FILENAME}"
apt-get update
export DEBIAN_FRONTEND="noninteractive"
apt-get install -y percona-server-server-5.7 libmysqlclient-dev
echo "CREATE USER 'mysql_user'@'%' IDENTIFIED BY 'mysql_password'; GRANT ALL PRIVILEGES ON *.* TO 'mysql_user'@'%'; FLUSH PRIVILEGES;" | mysql -u root

# System tweaks
printf "\nSetting /etc/environment\n"
{
  GOROOT='/usr/local/go'
  GOPATH='/vagrant'
  echo "GOROOT=${GOROOT}"
  echo "GOPATH=${GOPATH}"
  echo "PATH=${PATH}:${GOROOT}/bin:${GOPATH}/bin"
  echo "VITESS_WORKSPACE=/vagrant/src/vitess.io/vitess"
} >> /etc/environment
# shellcheck disable=SC2013
# shellcheck disable=SC2163
for line in $( cat /etc/environment ) ; do export "$line" ; done # source environment file

printf "\nSetting higher limit for max number of open files\n"
echo "fs.file-max = 10000" >> /etc/sysctl.conf
sysctl -p

# Set vitess env in .bashrc
cat /vagrant/src/vitess.io/vitess/vagrant-scripts/vagrant-bashrc  >> /home/vagrant/.bashrc

# Provisioning completed
touch $SEED_FILE
printf "\nProvisioning completed!\n\n"

#!/bin/bash
sudo apt-get install -y make unzip g++ curl git wget ant openjdk-8-jdk eatmydata
sudo service mysql stop
sudo bash -c "echo '/usr/sbin/mysqld { }' > /etc/apparmor.d/usr.sbin.mysqld" # https://bugs.launchpad.net/ubuntu/+source/mariadb-10.1/+bug/1806263
sudo ln -s /etc/apparmor.d/usr.sbin.mysqld /etc/apparmor.d/disable/
sudo apparmor_parser -R /etc/apparmor.d/usr.sbin.mysqld || echo "could not remove mysqld profile"
sudo apt-get install -y maven zip

mkdir -p dist bin
curl -L https://github.com/coreos/etcd/releases/download/v3.3.10/etcd-v3.3.10-linux-amd64.tar.gz | tar -zxC dist
mv dist/etcd-v3.3.10-linux-amd64/{etcd,etcdctl} bin/

go mod download
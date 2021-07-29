#!/bin/bash
sudo rm -rf /var/lib/mysql
sudo apt install -y gnupg2
wget https://repo.percona.com/apt/percona-release_latest."$(lsb_release -sc)"_all.deb
sudo dpkg -i percona-release_latest."$(lsb_release -sc)"_all.deb
sudo apt update
sudo DEBIAN_FRONTEND="noninteractive" apt-get install -y percona-server-server-5.6 percona-server-client-5.6
sudo rm ./*_all.deb
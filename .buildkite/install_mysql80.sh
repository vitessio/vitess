#!/bin/bash
wget -c https://dev.mysql.com/get/mysql-apt-config_0.8.14-1_all.deb
echo mysql-apt-config mysql-apt-config/select-server select mysql-8.0 | sudo debconf-set-selections
sudo DEBIAN_FRONTEND="noninteractive" dpkg -i mysql-apt-config*
sudo apt-get update
sudo DEBIAN_FRONTEND="noninteractive" apt-get install -y mysql-server mysql-client
sudo rm mysql-apt-config_0.8.14-1_all.deb
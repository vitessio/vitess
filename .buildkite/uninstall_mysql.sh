#!/bin/bash
sudo systemctl stop apparmor
sudo DEBIAN_FRONTEND="noninteractive" apt-get remove -y --purge mysql-server mysql-client mysql-common
sudo apt-get -y autoremove
sudo apt-get -y autoclean
sudo deluser mysql
sudo rm -rf /var/lib/mysql
sudo rm -rf /etc/mysql
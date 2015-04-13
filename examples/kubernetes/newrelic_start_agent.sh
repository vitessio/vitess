#!/bin/bash

NEWRELIC_LICENSE_KEY=$1

sudo sh -c 'echo deb http://apt.newrelic.com/debian/ newrelic non-free >> /etc/apt/sources.list.d/newrelic.list'
wget -O- https://download.newrelic.com/548C16BF.gpg | sudo apt-key add -
sudo apt-get update
sudo apt-get install newrelic-sysmond
sudo nrsysmond-config --set license_key=$NEWRELIC_LICENSE_KEY
sudo /etc/init.d/newrelic-sysmond start

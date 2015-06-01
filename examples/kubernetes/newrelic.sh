#!/bin/bash

NEWRELIC_LICENSE_KEY=$1

./newrelic_start_agent.sh $NEWRELIC_LICENSE_KEY
sudo cp newrelic_start_agent.sh /ssd
sudo cp newrelic_start_mysql_plugin.sh /ssd
mysql_docker_image=`sudo docker ps | awk '$NF~/^k8s_mysql/ {print $1}'`
vttablet_docker_image=`sudo docker ps | awk '$NF~/^k8s_vttablet/ {print $1}'`
vtgate_docker_image=`sudo docker ps | awk '$NF~/^k8s_vtgate/ {print $1}'`
for image in `echo -e "$mysql_socker_image\n$vttablet_docker_image\n$vtgate_docker_image"`; do
  sudo docker exec $image apt-get update
  sudo docker exec $image apt-get install sudo -y
  sudo docker exec $image apt-get install procps -y
  sudo docker exec $image bash /vt/vtdataroot/newrelic_start_agent.sh $NEWRELIC_LICENSE_KEY
done

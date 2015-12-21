#!/bin/bash

NEWRELIC_LICENSE_KEY=$1
VTDATAROOT=$2

./newrelic_start_agent.sh $NEWRELIC_LICENSE_KEY
if [ -n "$VTDATAROOT" ]; then
  sudo cp newrelic_start*.sh $VTDATAROOT
fi

mysql_docker_image=`sudo docker ps | awk '$NF~/^k8s_mysql/ {print $1}'`
vttablet_docker_image=`sudo docker ps | awk '$NF~/^k8s_vttablet/ {print $1}'`
vtgate_docker_image=`sudo docker ps | awk '$NF~/^k8s_vtgate/ {print $1}'`
for image in `echo -e "$mysql_docker_image\n$vttablet_docker_image\n$vtgate_docker_image"`; do
  if [ -z "$VTDATAROOT" ]; then
    vtdataroot=`sudo docker inspect -f '{{index .Volumes "/vt/vtdataroot"}}' $image`
    sudo cp newrelic_start*.sh $vtdataroot
  fi
  sudo docker exec $image apt-get update
  sudo docker exec $image apt-get install sudo -y
  sudo docker exec $image apt-get install procps -y
  sudo docker exec $image bash /vt/vtdataroot/newrelic_start_agent.sh $NEWRELIC_LICENSE_KEY
done

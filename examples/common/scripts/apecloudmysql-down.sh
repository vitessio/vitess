#!/bin/bash

docker stop mysql-server1
docker stop mysql-server2
docker stop mysql-server3

docker rm mysql-server1
docker rm mysql-server2
docker rm mysql-server3


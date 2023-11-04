#!/bin/bash

cd "$(dirname "$0")"
docker build --tag mysql-collation-data .

imgid=$(docker create mysql-collation-data)
docker cp $imgid:/mysql-collations/. ../../testdata/mysqldata
docker rm -v $imgid
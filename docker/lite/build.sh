#!/bin/bash

# This is the script to build the vitess/lite Docker image by extracting
# the pre-built binaries from a vitess/base image.

flavor=$1

if [[ -n "$flavor" ]]; then
  base_image=vitess/base:$flavor
else
  echo "Flavor not specified as first argument. Building default image."
  base_image=vitess/base
fi

# Extract files from vitess/base image
mkdir base
sudo docker run -ti --rm -v $PWD/base:/base -u root $base_image bash -c 'cp -R /vt /base/'

# Grab only what we need
lite=$PWD/lite
vttop=vt/src/github.com/youtube/vitess
mkdir -p $lite/vt/vtdataroot

mkdir -p $lite/vt/bin
(cd base/vt/bin; cp mysqlctld vtctld vtgate vttablet vtworker $lite/vt/bin/)

cp -R base/vt/dist lite/vt/

mkdir -p $lite/$vttop/go/cmd/vtctld
mkdir -p $lite/$vttop/web
cp -R base/$vttop/web/vtctld $lite/$vttop/web/

mkdir -p $lite/$vttop/config
cp -R base/$vttop/config/* $lite/$vttop/config/
ln -s /$vttop/config $lite/vt/config

sudo rm -rf base

# Fix permissions for AUFS workaround
chmod -R o=g lite

# Build vitess/lite image

if [[ -n "$flavor" ]]; then
	sudo docker build -f Dockerfile.$flavor -t vitess/lite:$flavor .
else
	sudo docker build -t vitess/lite .
fi

# Clean up temporary files
rm -rf lite

#!/bin/bash

# This script downloads and installs the grpc library, for
# go and python, in the root of the image. It assumes we're running
# as root in the image.
set -ex

# grpc_dist can be empty, in which case we just install to the default paths
grpc_dist="$1"
if [ "$grpc_dist" != "" ]; then
  cd $grpc_dist
fi

git clone https://github.com/grpc/grpc.git
cd grpc
git submodule update --init
make
if [ "$grpc_dist" != "" ]; then
  make install prefix=$grpc_dist
else
  make install
fi
CONFIG=opt ./tools/run_tests/build_python.sh
if [ "$grpc_dist" != "" ]; then
  pip install -r src/python/requirements.txt -t $grpc_dist/lib/python2.7/site-packages
  CFLAGS=-I$grpc_dist/include LDFLAGS=-L$grpc_dist/lib pip install src/python/src -t $grpc_dist/lib/python2.7/site-packages
else
  pip install -r src/python/requirements.txt
  pip install src/python/src
fi

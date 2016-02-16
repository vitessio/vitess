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

# for python, we'll need the latest virtualenv and tox.
# running gRPC requires the six package, any version will do.
if [ "$grpc_dist" != "" ]; then
  pip install --upgrade --root $grpc_dist --ignore-installed virtualenv tox
  pip install --root $grpc_dist --ignore-installed six
else
  pip install --upgrade virtualenv tox
  pip install six
fi

# clone the repository, setup the submodules
git clone https://github.com/grpc/grpc.git
cd grpc
git checkout release-0_12_0
git submodule update --init

# on OSX beta-1 doesn't work, it has to be built in version beta-2
if [ `uname -s` == "Darwin" ]; then
  cd third_party/protobuf
  git checkout v3.0.0-beta-2
  cd ../..

  # grpc with protobuf beta-2 fix (https://github.com/jtattermusch/grpc/commit/da717f464d667aca410f3a0ddeaa7ab45d34b7d3)
  sed -i -- 's/GetUmbrellaClassName/GetReflectionClassName/g' ./src/compiler/csharp_generator.cc
fi

# build everything
make

# install protobuf side (it was already built by the 'make' earlier)
cd third_party/protobuf
if [ "$grpc_dist" != "" ]; then
  make install prefix=$grpc_dist/usr/local
else
  make install
fi

# build and install python protobuf side
cd python
if [ "$grpc_dist" != "" ]; then
  python setup.py build --cpp_implementation
  python setup.py install --cpp_implementation --root=$grpc_dist
else
  python setup.py build --cpp_implementation
  python setup.py install --cpp_implementation
fi

# now install grpc itself
cd ../../..
if [ "$grpc_dist" != "" ]; then
  make install prefix=$grpc_dist/usr/local
else
  make install
fi

# and now build and install gRPC python libraries
# Note: running this twice as the first run exists
# with 'build_data' not found error. Seems the python
# libraries still work though.
CONFIG=opt ./tools/run_tests/build_python.sh || CONFIG=opt ./tools/run_tests/build_python.sh
if [ "$grpc_dist" != "" ]; then
  CFLAGS=-I$grpc_dist/include LDFLAGS=-L$grpc_dist/lib pip install src/python/grpcio --root $grpc_dist
else
  pip install src/python/grpcio
fi

# Build PHP extension, only in Travis.
if [ "$TRAVIS" == "true" ]; then
  echo "Building gRPC PHP extension..."
  eval "$(phpenv init -)"
  cd $grpc_dist/grpc/src/php/ext/grpc
  phpize
  ./configure --enable-grpc=$grpc_dist/usr/local
  make
  mkdir -p $HOME/.phpenv/lib
  mv modules/grpc.so $HOME/.phpenv/lib/
  echo "extension=$HOME/.phpenv/lib/grpc.so" > ~/.phpenv/versions/$(phpenv global)/etc/conf.d/grpc.ini
fi

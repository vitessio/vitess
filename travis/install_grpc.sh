#!/bin/bash

# This script downloads and installs the grpc library, for
# go and python, in the root of the image. It assumes we're running
# as root in the image.
set -ex

# grpc_dist can be empty, in which case we just install to the default paths
grpc_dist="$1"
if [ -n "$grpc_dist" ]; then
  cd $grpc_dist
fi

# for python, we'll need the latest virtualenv and tox.
# running gRPC requires the six package, version >=1.10.
if [ -n "$grpc_dist" ]; then
  # Create a virtualenv, which also creates a virualenv-boxed pip.
  virtualenv $grpc_dist/usr/local
  $grpc_dist/usr/local/bin/pip install --upgrade --ignore-installed virtualenv tox six
else
  pip install --upgrade --ignore-installed virtualenv tox six
fi

# clone the repository, setup the submodules
git clone https://github.com/grpc/grpc.git
cd grpc
git checkout release-0_13_0
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
if [ -n "$grpc_dist" ]; then
  make install prefix=$grpc_dist/usr/local
else
  make install
fi

# build and install python protobuf side
cd python
if [ -n "$grpc_dist" ]; then
  python setup.py build --cpp_implementation
  python setup.py install --cpp_implementation --prefix=$grpc_dist/usr/local
else
  python setup.py build --cpp_implementation
  python setup.py install --cpp_implementation
fi

# now install grpc itself
cd ../../..
if [ -n "$grpc_dist" ]; then
  make install prefix=$grpc_dist/usr/local
else
  make install
fi

# and now build and install gRPC python libraries
if [ -n "$grpc_dist" ]; then
  $grpc_dist/usr/local/bin/pip install .
else
  pip install .
fi

# Build PHP extension, only if requested.
if [ -n "$INSTALL_GRPC_PHP" ]; then
  echo "Building gRPC PHP extension..."
  cd src/php/ext/grpc
  phpize
  ./configure --enable-grpc=$grpc_dist/usr/local
  make
  mkdir -p $INSTALL_GRPC_PHP
  mv modules/grpc.so $INSTALL_GRPC_PHP
fi

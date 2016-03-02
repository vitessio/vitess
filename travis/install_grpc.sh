#!/bin/bash

# This script downloads and installs the grpc library, for
# go and python, in the root of the image. It assumes we're running
# as root in the image.
set -ex

# Import prepend_path function.
dir="$(dirname "${BASH_SOURCE[0]}")"
source "${dir}/../tools/shell_functions.inc"
if [ $? -ne 0 ]; then
  echo "failed to load ../tools/shell_functions.inc"
  return 1
fi

# grpc_dist can be empty, in which case we just install to the default paths
grpc_dist="$1"
if [ -n "$grpc_dist" ]; then
  cd $grpc_dist
fi

# Python requires a very recent version of virtualenv.
if [ -n "$grpc_dist" ]; then
  # Create a virtualenv, which also creates a virualenv-boxed pip.
  virtualenv $grpc_dist/usr/local
  $grpc_dist/usr/local/bin/pip install --upgrade --ignore-installed virtualenv
else
  pip install --upgrade --ignore-installed virtualenv
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

# now install grpc itself
cd ../..
if [ -n "$grpc_dist" ]; then
  make install prefix=$grpc_dist/usr/local

  # Add bin directory to the path such that gRPC python won't complain that
  # it cannot find "grpc_python_plugin".
  export PATH=$(prepend_path $PATH $grpc_dist/usr/local/bin)
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

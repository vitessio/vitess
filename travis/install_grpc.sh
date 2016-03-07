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
  # system wide installations require an explicit upgrade of
  # certain gRPC Python dependencies e.g. "six" on Debian Jessie.
  pip install --upgrade --ignore-installed six
fi

# clone the repository, setup the submodules
git clone https://github.com/grpc/grpc.git
cd grpc
git checkout release-0_13_0
git submodule update --init

# OSX specific setting + dependencies
if [ `uname -s` == "Darwin" ]; then
  export GRPC_PYTHON_BUILD_WITH_CYTHON=1
  $grpc_dist/usr/local/bin/pip install Cython
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

# Pin the protobuf python dependency to a specific version which is >=3.0.0a3.
#
# This prevents us from running into the bug that the protobuf package with the
# version "3.0.0-alpha-1" is treated as newer than "3.0.0a3" (alpha-3).
# See: https://github.com/google/protobuf/issues/855
# Also discussed here: https://github.com/grpc/grpc/issues/5534
# In particular, we hit this issue on Travis.
sed -i -e 's/protobuf>=3.0.0a3/protobuf==3.0.0a3/' setup.py

# and now build and install gRPC python libraries
# (Dependencies like protobuf python will be installed automatically.)
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

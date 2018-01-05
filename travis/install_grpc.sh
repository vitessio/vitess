#!/bin/bash

# Copyright 2017 Google Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

cd $grpc_dist

# Python requires a very recent version of virtualenv.
# We also require a recent version of pip, as we use it to
# upgrade the other tools.
# For instance, setuptools doesn't work with pip 6.0:
# https://github.com/pypa/setuptools/issues/945
# (and setuptools is used by grpc install).
$VIRTUALENV -v $grpc_dist/usr/local
PIP=$grpc_dist/usr/local/bin/pip
$PIP install --upgrade pip
$PIP install --upgrade --ignore-installed virtualenv

# clone the repository, setup the submodules
git clone https://github.com/grpc/grpc.git
cd grpc
git checkout $grpc_ver
git submodule update --init

# OSX specific setting + dependencies
if [ `uname -s` == "Darwin" ]; then
     export GRPC_PYTHON_BUILD_WITH_CYTHON=1
     $PIP install Cython

     # Work-around macOS Sierra blocker, see: https://github.com/youtube/vitess/issues/2115
     # TODO(mberlin): Remove this when the underlying issue is fixed and available
     #                in the gRPC version used by Vitess.
     #                See: https://github.com/google/protobuf/issues/2182
     export CPPFLAGS="-Wno-deprecated-declarations"
fi

# Install gRPC python libraries from PyPI.
# Dependencies like protobuf python will be installed automatically.
grpcio_ver=1.7.0
$PIP install --upgrade grpcio==$grpcio_ver

# now install grpc_python_plugin, which also builds the protoc compiler.
make grpc_python_plugin
ln -sf $grpc_dist/grpc/bins/opt/protobuf/protoc $VTROOT/bin/protoc
ln -sf $grpc_dist/grpc/bins/opt/protobuf/grpc_python_plugin $VTROOT/bin/grpc_python_plugin

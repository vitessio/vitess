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

# grpc_dist can be empty, in which case we just install to the default paths
grpc_dist="$1"
if [ -n "$grpc_dist" ]; then
  cd $grpc_dist
fi

if [[ -z "$PIP" ]]; then
  # PIP is not set i.e. dev.env was not loaded.
  # We're probably doing a system-wide installation when building the Docker
  # bootstrap image. Set the variable now.
  PIP=pip
fi

# Python requires a very recent version of virtualenv.
# We also require a recent version of pip, as we use it to
# upgrade the other tools.
# For instance, setuptools doesn't work with pip 6.0:
# https://github.com/pypa/setuptools/issues/945
# (and setuptools is used by grpc install).
if [ -n "$grpc_dist" ]; then
  # Non-system wide installation. Create a virtualenv, which also creates a
  # virtualenv-boxed pip.

  # Update both pip and virtualenv.
  $VIRTUALENV -v $grpc_dist/usr/local
  PIP=$grpc_dist/usr/local/bin/pip
  $PIP install --upgrade pip
  $PIP install --upgrade --ignore-installed virtualenv
fi

# Install gRPC python libraries from PyPI.
# Dependencies like protobuf python will be installed automatically.
grpcio_ver=1.7.0
$PIP install --upgrade grpcio==$grpcio_ver

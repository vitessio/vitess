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

# This is the script to build the vitess/etcd-lite Docker image by extracting
# the pre-built binaries from a vitess/etcde image.

version="v2.0.13"

set -e

# Build a fresh base vitess/etcd image
(cd ../etcd ; docker build -t vitess/etcd:$version .)

# Extract files from vitess/etcd image
mkdir base
docker run -ti --rm -v $PWD/base:/base -u $UID vitess/etcd:$version bash -c 'cp -R /go/bin/* /base/'

# Build vitess/etcd-lite image
docker build -t vitess/etcd:$version-lite .

# Clean up temporary files
rm -rf base

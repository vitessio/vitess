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

# This is a script to build the vitess/guestbook Docker image.
# It must be run from within a bootstrapped vitess tree, after dev.env.

set -e

mkdir tmp tmp/pkg tmp/lib
cp extract.sh tmp/
chmod -R 777 tmp

# We also need the grpc library.
docker run --rm -v $PWD/tmp:/out vitess/base bash /out/extract.sh

# Build the Docker image.
docker build -t vitess/guestbook .

# Clean up.
docker run --rm -v $PWD/tmp:/out vitess/base bash -c 'rm -rf /out/pkg/* /out/lib/*'
rm -rf tmp

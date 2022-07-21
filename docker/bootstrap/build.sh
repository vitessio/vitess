#!/bin/bash

# Copyright 2019 The Vitess Authors.
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
#
# Usage:
#
# First build the `common` image, then any flavors you want. For example:
# $ docker/bootstrap/build.sh common
# $ docker/bootstrap/build.sh mysql80
#
# Is it also possible to specify the resulting image name:
# $ docker/bootstrap/build.sh common --image my-common-image
#
# If custom image names are specified, you might need to set the base image name when building flavors:
# $ docker/bootstrap/build.sh mysql80 --base_image my-common-image
# Both arguments can be combined. For example:
# $ docker/bootstrap/build.sh mysql80 --base_image my-common-image --image my-mysql-image


flavor=$1
if [[ -z "$flavor" ]]; then
  echo "Flavor must be specified as first argument."
  exit 1
fi

# Set default version of 0
version="${2:-0}"

if [[ ! -f bootstrap.sh ]]; then
  echo "This script should be run from the root of the Vitess source tree - e.g. ~/src/vitess.io/vitess"
  exit 1
fi

# Fix permissions before copying files, to avoid AUFS bug other must have read/access permissions
chmod -R o=rx *;

arch=$(uname -m)
[ "$arch" == "aarch64" ] && [ $flavor != "common" ] && arch_ext='-arm64v8'


base_image="${base_image:-vitess/bootstrap:$version-common}"
image="${image:-vitess/bootstrap:$version-$flavor$arch_ext}"

while [ $# -gt 0 ]; do
   if [[ $1 == *"--"* ]]; then
        param="${1/--/}"
        declare $param="$2"
   fi
  shift
done

if [ -f "docker/bootstrap/Dockerfile.$flavor$arch_ext" ]; then
    docker build --no-cache -f docker/bootstrap/Dockerfile.$flavor$arch_ext -t $image --build-arg bootstrap_version=$version --build-arg image=$base_image .
fi

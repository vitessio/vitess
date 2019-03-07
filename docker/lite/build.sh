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

# This is the script to build the vitess/lite Docker image by extracting
# the pre-built binaries from a vitess/base image.

set -ex

# Parse command line arguments.
prompt_notice=true
if [[ "$1" == "--prompt"* ]]; then
  if [[ "$1" == "--prompt=false" ]]; then
    prompt_notice=false
  fi
  shift
fi

flavor=$1
base_image=vitess/base
lite_image=vitess/lite
dockerfile=Dockerfile
tag=latest

if [[ -n "$flavor" ]]; then
  lite_image=vitess/lite:$flavor
  dockerfile=Dockerfile.$flavor
  tag=$flavor
else
  echo "Flavor not specified as first argument. Building default image."
fi

# Abort if base image does not exist.
if ! docker inspect $base_image &>/dev/null; then
  echo "ERROR: Dependent image $base_image does not exist. Run 'make $make_target' to build it locally or 'docker pull $base_image' to fetch it from Docker Hub (if it is published)."
  exit 1
fi

# Educate the user that they have to build or pull vitess/base themselves.
if [[ "$prompt_notice" = true ]]; then
  cat <<END

This script is going to repack and copy the existing *local* base image '$base_image' into a smaller image '$lite_image'.

It does NOT recompile the Vitess binaries. For that you will have to rebuild or pull the base image.

The 'docker images' output below shows you how old your local base image is:

$(docker images vitess/base | grep -E "(CREATED|$tag)")

If you need a newer base image, you will have to manually run 'make $make_target' to build it locally
or 'docker pull $base_image' to fetch it from Docker Hub.

Press ENTER to continue building '$lite_image' or Ctrl-C to cancel.
END
  read
fi

docker build --no-cache -f $dockerfile -t $lite_image .

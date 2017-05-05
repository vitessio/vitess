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
make_target=docker_base
lite_image=vitess/lite
dockerfile=Dockerfile
tag=latest
if [[ -n "$flavor" ]]; then
  base_image=vitess/base:$flavor
  make_target=docker_base_$flavor
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

# Extract files from vitess/base image
mkdir base
# Ignore permission errors. They occur for directories we do not care e.g. ".git".
# (Copying them fails because they are owned by root and not $UID and have stricter permissions.)
docker run -ti --rm -v $PWD/base:/base -u $UID $base_image bash -c 'cp -R /vt /base/ 2>&1 | grep -v "Permission denied"'

# Grab only what we need
lite=$PWD/lite
vttop=vt/src/github.com/youtube/vitess
mkdir -p $lite/vt/vtdataroot

mkdir -p $lite/vt/bin
(cd base/vt/bin; cp mysqlctld vtctld vtgate vttablet vtworker $lite/vt/bin/)

mkdir -p $lite/$vttop/web
cp -R base/$vttop/web/vtctld $lite/$vttop/web/
mkdir $lite/$vttop/web/vtctld2
cp -R base/$vttop/web/vtctld2/app $lite/$vttop/web/vtctld2/

mkdir -p $lite/$vttop/config
cp -R base/$vttop/config/* $lite/$vttop/config/
ln -s /$vttop/config $lite/vt/config

rm -rf base

# Fix permissions for AUFS workaround
chmod -R o=g lite

# Build vitess/lite image
docker build --no-cache -f $dockerfile -t $lite_image .

# Clean up temporary files
rm -rf lite

#!/bin/bash

# Copyright 2013, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

if [ ! -f bootstrap.sh ]; then
  echo "[ERROR] bootstrap.sh must be run from its current directory" 1>&2
  exit 1
fi

dir=${PWD}

cd ..
. ./dev.env

# Check for prerequisites:
echo [ INFO ] Checking for prerequisites...
[[ $(sbt --version) != sbt* ]] && echo [ERROR] Install sbt 1>&2 && exit 1
[[ $(mvn --version) != *Maven* ]] && echo [ERROR] Install maven 1>&2 && exit 1
[[ $(git --version) != git* ]] && echo [ERROR] Install git 1>&2 && exit 1

acolyte_dist=$VTROOT/dist/acolyte-core
acolyte=$VTTOP/third_party/acolyte
vtocc_jars="$(find ~/.m2/repository/com/github/youtube/vitess/vtocc-jdbc-driver | grep jar)"
if [ ! -z "$vtocc_jars" ]; then
  echo "[INFO] Skipping vtocc-jdbc-driver build, jars found: $vtocc_jars"
else
  set -e
  echo "[INFO] Getting and compiling acolyte-core"
  rm -rf $acolyte
  git clone https://github.com/cchantep/acolyte.git $acolyte
  cd $acolyte
  git checkout tags/1.0.13
  git apply ../acolyte.patch --ignore-whitespace
  sbt publish
  mkdir -p $acolyte_dist
  mv $acolyte/core/target/*.jar $acolyte_dist
  echo "[INFO] acolyte-core is successfully installed"
  echo "[INFO] Rebuilding and installing vtocc-jdbc-driver"
  cd $dir/vtocc-jdbc-driver
  mvn dependency:copy-dependencies install
  echo "[INFO] vtocc-jdbc-driver is successfully installed"
  set +e
fi

#!/bin/bash

# Copyright 2014, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# Check for prerequisites:
echo "[INFO] Checking for prerequisites..."
[ -z "$VTTOP" ] && echo "[ERROR] source dev.env first, VTTOP is empty" 1>&2 && exit 1
[ -z "$VTROOT" ] && echo "[ERROR] source dev.env first, VTROOT is empty" 1>&2 && exit 1
[ -z "$VTDATAROOT" ] && echo "[ERROR] source dev.env first, VTDATAROOT is empty" 1>&2 && exit 1
type sbt >/dev/null 2>&1 || { echo >&2 "[ERROR] Install sbt from http://www.scala-sbt.org/release/tutorial/Installing-sbt-on-Linux.html. Aborting."; exit 1; }
type protoc >/dev/null 2>&1 || { echo >&2 "[ERROR] Install protoc with sudo apt-get install protobuf-compiler. Aborting."; exit 1; }
type mvn >/dev/null 2>&1 || { echo >&2 "[ERROR] Install maven with sudo apt-get install maven. Aborting."; exit 1; }
type git >/dev/null 2>&1 || { echo >&2 "[ERROR] Install git with sudo apt-get install git. Aborting."; exit 1; }

ACOLYTE_DIST="$VTROOT/dist/java/org/eu/acolyte/acolyte-core/1.0.13-PATCHED"
ACOLYTE="$VTTOP/third_party/acolyte"
VTOCC_JARS="$(find ~/.m2/repository/com/github/youtube/vitess/vtocc-jdbc-driver | grep .jar)"
if [ ! -z "$VTOCC_JARS" ]; then
  echo "[INFO] Skipping vtocc-jdbc-driver build, jars found: $VTOCC_JARS"
else
  set -e
  echo "[INFO] Getting and compiling acolyte-core"
  rm -rf "$ACOLYTE"
  git clone https://github.com/cchantep/acolyte.git "$ACOLYTE"
  cd "$ACOLYTE"
  git checkout tags/1.0.13
  git apply ../acolyte.patch --ignore-whitespace
  sbt publish
  mkdir -p "$ACOLYTE_DIST"
  mv "$ACOLYTE"/core/target/acolyte-core-* "$ACOLYTE_DIST"
  echo "[INFO] acolyte-core is successfully installed"
  echo "[INFO] Rebuilding and installing vtocc-jdbc-driver"
  cd "$VTTOP/java"
  mvn install -DVTTOP=$VTTOP -DVTROOT=$VTROOT -DVTDATAROOT=$VTDATAROOT
  echo "[INFO] vtocc-jdbc-driver is successfully installed"
  set +e
fi

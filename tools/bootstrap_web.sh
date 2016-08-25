#!/bin/bash

# Copyright 2016, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# This script is used to install dependencies for compiling
# the code of our upcoming Angular 2 based vtctld UI.
#
# Regular users should not have to run it. Run bootstrap.sh (located in the
# repository root) instead.

# TODO(mberlin): Merge this back into bootstrap.sh once we support caching the
#                dependencies on Travis and local disk.

# Download node
node_ver=v6.3.1
node_dist=$VTROOT/dist/node
if [[ -x $node_dist/bin/node && `$node_dist/bin/node -v` == "$node_ver" ]]; then
  echo "skipping nodejs download. remove $node_dist to force redownload."
else
  echo "Downloading nodejs"
  rm -rf $node_dist
  curl -sL https://nodejs.org/dist/$node_ver/node-$node_ver-linux-x64.tar.xz > node_linux64.xz
  tar xf node_linux64.xz -C $VTROOT/dist
  mv $VTROOT/dist/node-$node_ver-linux-x64 $node_dist
  rm node_linux64.xz
fi

echo "Installing dependencies for building web UI"
angular_cli_dir=$VTROOT/dist/angular-cli
web_dir2=$VTTOP/web/vtctld2
rm -rf $angular_cli_dir
cd $VTROOT/dist && git clone https://github.com/angular/angular-cli.git --quiet
cd $angular_cli_dir && git checkout 3dcd49bc625db36dd9f539cf9ce2492107e0258c --quiet
cd $angular_cli_dir && $node_dist/bin/npm link --silent
$node_dist/bin/npm install -g bower --silent
cd $web_dir2 && $node_dist/bin/npm install --silent
cd $web_dir2 && $node_dist/bin/npm link angular-cli --silent
cd $web_dir2 && $node_dist/bin/bower install --silent

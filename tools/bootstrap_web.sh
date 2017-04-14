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
  node_tar="node_linux64.tar.xz"
  curl -sL https://nodejs.org/dist/$node_ver/node-$node_ver-linux-x64.tar.xz -o $node_tar
  tar xf $node_tar -C $VTROOT/dist
  mv $VTROOT/dist/node-$node_ver-linux-x64 $node_dist
  rm $node_tar
  # Add the node directory to PATH to make sure that the Angular
  # installation below can find the "node" binary.
  # (dev.env does actually append it to PATH.)
  source $VTTOP/dev.env
fi

echo "Installing dependencies for building web UI"
angular_cli_dir=$VTROOT/dist/angular-cli
web_dir2=$VTTOP/web/vtctld2
angular_cli_commit=cacaa4eff10e135016ef81076fab1086a3bce92f
if [[ -d $angular_cli_dir && `cd $angular_cli_dir && git rev-parse HEAD` == "$angular_cli_commit" ]]; then
  echo "skipping angular cli download. remove $angular_cli_dir to force download."
else
  cd $VTROOT/dist && git clone https://github.com/angular/angular-cli.git --quiet
  cd $angular_cli_dir && git checkout $angular_cli_commit --quiet
fi
cd $angular_cli_dir && $node_dist/bin/npm link --silent
cd $web_dir2 && $node_dist/bin/npm install --silent
cd $web_dir2 && $node_dist/bin/npm link angular-cli --silent

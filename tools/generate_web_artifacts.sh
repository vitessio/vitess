#!/bin/bash

# Copyright 2016, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# This script is used to build and copy the Angular 2 based vtctld UI
# into the release folder (app) for checkin. Prior to running this script,
# bootstrap.sh and bootstrap_web.sh should already have been run.

set -e

vtctld2_dir=$VTTOP/web/vtctld2
if [[ -d $vtctld2_dir/app ]]; then
  rm -rf $vtctld2_dir/app
fi
cd $vtctld2_dir && ng build -prod --output-path app/
rm -rf $vtctld2_dir/app/assets
cp -f $vtctld2_dir/src/{favicon.ico,plotly-latest.min.js,primeui-ng-all.min.css} $vtctld2_dir/app/

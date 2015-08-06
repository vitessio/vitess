#!/bin/bash

# Copyright 2015, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $DIR/shell_functions.inc

# Starting with Go 1.5, the syntax for the -X flag changed.
# Earlier Go versions don't support the new syntax.
if goversion_min 1.5; then
	echo "\
	-X 'github.com/youtube/vitess/go/vt/servenv.buildHost=$(hostname)'\
	-X 'github.com/youtube/vitess/go/vt/servenv.buildUser=$(whoami)'\
	-X 'github.com/youtube/vitess/go/vt/servenv.buildGitRev=$(git rev-parse HEAD)'\
	-X 'github.com/youtube/vitess/go/vt/servenv.buildTime=$(LC_ALL=C date)'\
	"
else
	echo "\
	-X github.com/youtube/vitess/go/vt/servenv.buildHost   '$(hostname)'\
	-X github.com/youtube/vitess/go/vt/servenv.buildUser   '$(whoami)'\
	-X github.com/youtube/vitess/go/vt/servenv.buildGitRev '$(git rev-parse HEAD)'\
	-X github.com/youtube/vitess/go/vt/servenv.buildTime   '$(LC_ALL=C date)'\
	"
fi

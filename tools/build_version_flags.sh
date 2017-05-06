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

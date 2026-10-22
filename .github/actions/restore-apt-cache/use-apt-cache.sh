#!/bin/bash

# Copyright 2026 The Vitess Authors.
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

# Points apt's archive directory at /var/cache/vitess-apt, so that apt
# installs the packages it finds there instead of downloading them, and keeps
# the ones it downloads. apt checks every cached file against the package
# index, so an outdated one is only downloaded again.

set -euo pipefail

dir=/var/cache/vitess-apt

# apt downloads into partial/ as its _apt user, then moves the files up.
sudo install -d -o _apt -g root -m 0700 "${dir}/partial"
sudo tee /etc/apt/apt.conf.d/99vitess-apt-cache > /dev/null <<CONF
Dir::Cache::Archives "${dir}/";
APT::Keep-Downloaded-Packages "true";
Binary::apt::APT::Keep-Downloaded-Packages "true";
CONF

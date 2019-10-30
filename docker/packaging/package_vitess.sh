#!/bin/bash

# Copyright 2019 The Vitess Authors.
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

if [ -z "${VERSION}" ]; then
  echo "Set the env var VERSION with the release version"
  exit 1
fi

set -eu

PREFIX=${PREFIX:-/usr}

inputs_file="/vt/packaging/inputs"
cat <<EOF >> "${inputs_file}"
/vt/bin/mysqlctld=${PREFIX}/bin/mysqlctld
/vt/bin/vtbackup=${PREFIX}/bin/vtbackup
/vt/bin/vtctl=${PREFIX}/bin/vtctl
/vt/bin/vtctlclient=${PREFIX}/bin/vtctlclient
/vt/bin/vtctld=${PREFIX}/bin/vtctld
/vt/bin/vtgate=${PREFIX}/bin/vtgate
/vt/bin/vttablet=${PREFIX}/bin/vttablet
/vt/bin/vtworker=${PREFIX}/bin/vtworker
/vt/src/vitess.io/vitess/config/=/etc/vitess
/vt/src/vitess.io/vitess/web/vtctld2/app=${PREFIX}/lib/vitess/web/vtcld2
/vt/src/vitess.io/vitess/web/vtctld=${PREFIX}/lib/vitess/web
/vt/src/vitess.io/vitess/examples/local/=${PREFIX}/share/vitess/examples
EOF

description='A database clustering system for horizontal scaling of MySQL

Vitess is a database solution for deploying, scaling and managing large
clusters of MySQL instances. It’s architected to run as effectively in a public
or private cloud architecture as it does on dedicated hardware. It combines and
extends many important MySQL features with the scalability of a NoSQL database.'

exec /usr/local/bin/fpm \
    --force \
    --input-type dir \
    --name vitess \
    --version "${VERSION}" \
    --url "https://vitess.io/" \
    --description "${description}" \
    --license "Apache License - Version 2.0, January 2004" \
    --inputs "${inputs_file}" \
    --config-files "/etc/vitess" \
    --directories "${PREFIX}/lib/vitess" \
    --before-install "/vt/packaging/preinstall.sh" \
    "${@}"

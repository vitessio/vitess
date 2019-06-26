#!/bin/bash

if [ -z "${VERSION}" ]; then
  echo "Set the env var VERSION with the release version"
  exit 1
fi

set -eu

inputs_file="/vt/packaging/inputs"
cat <<EOF >> "${inputs_file}"
/vt/bin/mysqlctld
/vt/bin/vtctlclient
/vt/bin/vtctld
/vt/bin/vtgate
/vt/bin/vttablet
/vt/bin/vtworker
/vt/packaging/etc_default_vitess=/etc/default/vitess
/vt/src/vitess.io/vitess/config=/vt
/vt/src/vitess.io/vitess/web/vtctld
/vt/src/vitess.io/vitess/web/vtctld2/app
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
    --config-files "/vt/config" \
    --config-files "/etc/default/vitess" \
    --directories "/vt/bin" \
    --directories "/vt/src" \
    --inputs "${inputs_file}" \
    --before-install "/vt/packaging/preinstall.sh" \
    --deb-user vitess \
    --deb-group vitess \
    --rpm-user vitess \
    --rpm-group vitess \
    "${@}"

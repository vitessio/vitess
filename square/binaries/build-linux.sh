#!/bin/bash

set -ex

godir="/vt"
vtdir="${godir}/src/vitess.io/vitess"

tar czf vitess-linux-build.tar.gz \
    -C ${godir} $(cd /$godir && find bin -name 'vt*') bin/mysqlctl bin/mysqlctld bin/zk \
    -C ${vtdir} web config

#!/bin/bash
# This script will create a vitess_distro.tgz file to be used with a terraform deploy

tar cfvz vitess_distro.tgz $GOPATH/src/vitess.io/vitess/config $GOPATH/src/vitess.io/vitess/web $GOPATH/src/vitess.io/vitess/build $GOPATH/bin/vt* $GOPATH/bin/mysqlctl*

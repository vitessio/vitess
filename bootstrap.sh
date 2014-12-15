#!/bin/bash

# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

if [ ! -f bootstrap.sh ]; then
  echo "bootstrap.sh must be run from its current directory" 1>&2
  exit 1
fi

. ./dev.env

mkdir -p $VTROOT/dist
mkdir -p $VTROOT/bin
mkdir -p $VTROOT/lib
mkdir -p $VTROOT/vthook

# install zookeeper
zk_dist=$VTROOT/dist/vt-zookeeper-3.3.5
if [ -d $zk_dist ]; then
  echo "skipping zookeeper build"
else
  (cd $VTTOP/third_party/zookeeper && \
    tar -xjf zookeeper-3.3.5.tbz && \
    mkdir -p $zk_dist/lib && \
    cp zookeeper-3.3.5/contrib/fatjar/zookeeper-3.3.5-fatjar.jar $zk_dist/lib && \
    (cd zookeeper-3.3.5/src/c && \
    ./configure --prefix=$zk_dist && \
    make -j3 install) && rm -rf zookeeper-3.3.5)
  if [ $? -ne 0 ]; then
    echo "zookeeper build failed"
    exit 1
   fi
fi

ln -nfs $VTTOP/third_party/go/launchpad.net $VTROOT/src
go install launchpad.net/gozk/zookeeper

go get code.google.com/p/goprotobuf/proto
go get golang.org/x/net/context
go get code.google.com/p/go.tools/cmd/goimports
go get github.com/golang/glog
go get github.com/coreos/go-etcd/etcd

ln -snf $VTTOP/config $VTROOT/config
ln -snf $VTTOP/data $VTROOT/data
ln -snf $VTTOP/py $VTROOT/py-vtdb
ln -snf $VTTOP/go/zk/zkctl/zksrv.sh $VTROOT/bin/zksrv.sh
ln -snf $VTTOP/test/vthook-test.sh $VTROOT/vthook/test.sh

# install mysql
case "$MYSQL_FLAVOR" in
  "MariaDB")
    myversion=`$VT_MYSQL_ROOT/bin/mysql --version | grep MariaDB`
    if [ "$myversion" == "" ]; then
      echo "Couldn't find MariaDB in $VT_MYSQL_ROOT. Set VT_MYSQL_ROOT to override search location."
      exit 1
    fi
    echo "Found MariaDB installation in $VT_MYSQL_ROOT."
    ;;

  *)
    export VT_MYSQL_ROOT=$VTROOT/dist/mysql
	if [ -d $VT_MYSQL_ROOT ]; then
      echo "Skipping Google MySQL build. Delete $VT_MYSQL_ROOT to force rebuild."
    else
      echo "Getting and compiling Google MySQL"
      git clone https://code.google.com/r/sougou-vitess-mysql/ third_party/mysql
      pushd third_party/mysql
      set -e
      git apply ../mysql.patch
      source google/env.inc
      source google/compile.inc

      # Install
      make -s install #DESTDIR=$VTROOT/dist/mysql
      rm -rf $VTROOT/dist/mysql/mysql-test
      rm -rf $VTROOT/dist/mysql/sql-bench
      popd
      rm -rf third_party/mysql
    fi
  ;;
esac

# save the flavor that was used in bootstrap, so it can be restored
# every time dev.env is sourced.
echo "$MYSQL_FLAVOR" > $VTROOT/dist/MYSQL_FLAVOR

# generate pkg-config, so go can use mysql C client
if [ ! -x $VT_MYSQL_ROOT/bin/mysql_config ]; then
  echo "Cannot execute $VT_MYSQL_ROOT/bin/mysql_config. Did you install a client dev package?" 1>&2
  exit 1
fi

cp $VTTOP/config/gomysql.pc.tmpl $VTROOT/lib/gomysql.pc
echo "Version:" "$($VT_MYSQL_ROOT/bin/mysql_config --version)" >> $VTROOT/lib/gomysql.pc
echo "Cflags:" "$($VT_MYSQL_ROOT/bin/mysql_config --cflags) -ggdb -fPIC" >> $VTROOT/lib/gomysql.pc
if [ "$MYSQL_FLAVOR" == "MariaDB" ]; then
  # Use static linking because the shared library doesn't export
  # some internal functions we use, like cli_safe_read.
  echo "Libs:" "$($VT_MYSQL_ROOT/bin/mysql_config --libs_r | sed 's,-lmysqlclient_r,-l:libmysqlclient.a -lstdc++,')" >> $VTROOT/lib/gomysql.pc
else
  echo "Libs:" "$($VT_MYSQL_ROOT/bin/mysql_config --libs_r)" >> $VTROOT/lib/gomysql.pc
fi

# install bson
bson_dist=$VTROOT/dist/py-vt-bson-0.3.2
if [ -d $bson_dist ]; then
  echo "skipping bson python build"
else
  cd $VTTOP/third_party/py/bson-0.3.2 && \
    python ./setup.py install --prefix=$bson_dist && \
    rm -r build
fi

# install cbson
cbson_dist=$VTROOT/dist/py-cbson
if [ -d $cbson_dist ]; then
  echo "skipping cbson python build"
else
  cd $VTTOP/py/cbson && \
    python ./setup.py install --prefix=$cbson_dist
fi

# create pre-commit hooks
echo "creating git pre-commit hooks"
ln -sf $VTTOP/misc/git/pre-commit $VTTOP/.git/hooks/pre-commit
echo "source dev.env in your shell to complete the setup."

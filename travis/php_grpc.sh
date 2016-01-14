#!/bin/bash

eval "$(phpenv init -)"

set -e

if [ -f $HOME/.phpenv/lib/grpc.so ]; then
  echo "Using cached grpc.so"
else
  cd $HOME/gopath/dist/grpc/grpc/src/php/ext/grpc
  phpize
  ./configure --enable-grpc=$HOME/gopath/dist/grpc
  make
  mkdir -p $HOME/.phpenv/lib
  mv modules/grpc.so $HOME/.phpenv/lib/
  echo "extension=$HOME/.phpenv/lib/grpc.so" > ~/.phpenv/versions/$(phpenv global)/etc/conf.d/grpc.ini
fi

# If you need to trigger a re-install of PHP dependencies,
# increment this value.
ver=3
ver_file=$HOME/.phpenv/vendor/travis.ver
if [[ -f $ver_file && "$(cat $ver_file)" == "$ver" ]]; then
  echo "Using cached php vendor dir version $ver"
else
  cd $HOME/gopath/src/github.com/youtube/vitess
  composer install
  rm -rf $HOME/.phpenv/vendor
  mv php/vendor $HOME/.phpenv/vendor
  echo "$ver" > $ver_file
fi

ln -s $HOME/.phpenv/vendor $HOME/gopath/src/github.com/youtube/vitess/php/vendor

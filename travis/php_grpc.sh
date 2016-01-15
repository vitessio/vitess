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

# composer install should be run in every travis build
cd $HOME/gopath/src/github.com/youtube/vitess
composer install

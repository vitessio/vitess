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

version=5.5.9

set -e

if [ -f $HOME/.phpenv/bin/phpenv ]; then
  echo "Using cached phpenv"
else
  curl -s https://raw.githubusercontent.com/CHH/phpenv/master/bin/phpenv-install.sh | bash
  git clone git://github.com/michael-berlin/php-build.git $HOME/.phpenv/plugins/php-build
fi

eval "$(phpenv init -)"

if [ -d $HOME/.phpenv/versions/$version ]; then
  echo "Using cached php"
else
  PHP_BUILD_CONFIGURE_OPTS="--without-xmlrpc --without-xsl --without-curl --without-gd --without-mcrypt --without-readline --disable-soap --without-tidy" phpenv install $version
fi

phpenv global $version
phpenv rehash

# gRPC plugin will be placed by install_grpc.sh
echo "extension=$HOME/.phpenv/lib/grpc.so" > ~/.phpenv/versions/$(phpenv global)/etc/conf.d/grpc.ini

if [ -f $HOME/.phpenv/bin/phpunit ]; then
  echo "Using cached phpunit"
else
  curl -sL https://phar.phpunit.de/phpunit-4.8.9.phar > $HOME/.phpenv/bin/phpunit
  chmod +x $HOME/.phpenv/bin/phpunit
fi

# If you need to trigger a re-download,
# increment this value.
ver=1
ver_file=$HOME/.phpenv/bin/composer.ver
if [[ -f $ver_file && "$(cat $ver_file)" == "$ver" ]]; then
  echo "Using cached composer"
else
  rm -f $HOME/.phpenv/bin/composer
  curl -sS https://getcomposer.org/installer | php -- --install-dir=$HOME/.phpenv/bin/ --filename=composer
  echo "$ver" > $ver_file
fi

# composer install should be run in every travis build
cd $HOME/gopath/src/github.com/youtube/vitess
composer install

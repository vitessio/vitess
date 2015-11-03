#!/bin/bash

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
	PHP_BUILD_CONFIGURE_OPTS="--without-xmlrpc --without-xsl --without-curl --disable-dom --without-gd --without-mcrypt --without-readline --disable-soap --without-tidy" phpenv install $version
fi

phpenv global $version
phpenv rehash

if [ -f $HOME/.phpenv/bin/phpunit ]; then
	echo "Using cached phpunit"
else
	curl -sL https://phar.phpunit.de/phpunit-4.8.9.phar > $HOME/.phpenv/bin/phpunit
	chmod +x $HOME/.phpenv/bin/phpunit
fi

if [ -f $HOME/.phpenv/bin/composer ]; then
	echo "Using cached composer"
else
	curl -sS https://getcomposer.org/installer | php -- --install-dir=$HOME/.phpenv/bin/ --filename=composer
fi

if [ ! -f $HOME/.phpenv/lib/grpc.so ]; then
	echo "Forcing rebuild of gRPC so we can build PHP extension"
	rm -rf $HOME/gopath/dist
fi

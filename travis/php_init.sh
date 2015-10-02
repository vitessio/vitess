#!/bin/bash

set -e

mkdir -p $HOME/php

if [ -f $HOME/php/phpunit ]; then
	echo "Using cached phpunit"
else
	curl -sL https://phar.phpunit.de/phpunit-4.8.9.phar > $HOME/php/phpunit
	chmod +x $HOME/php/phpunit
fi

if [ -f $HOME/php/mongo.so ]; then
	echo "Using cached mongo.so"
else
	mkdir -p $HOME/mongo
	git clone https://github.com/mongodb/mongo-php-driver.git $HOME/mongo
	cd $HOME/mongo
	phpize
	./configure
	make
	mv modules/mongo.so $HOME/php/
fi

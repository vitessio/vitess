# Vitess PHP

This folder contains the PHP client for Vitess.

For a simple example of using the API, see the [client.php]
(https://github.com/youtube/vitess/blob/master/examples/local/client.php)
script, which works with the environment created by the [local setup example]
(http://vitess.io/getting-started/local-instance.html).

There is also a similar example client for the [VTGateV3 demo]
(https://github.com/youtube/vitess/tree/master/examples/demo) environment.

## Prerequisites

PHP 5.5+ is required.

### gRPC Extension Module

Install the [gRPC extension module](https://pecl.php.net/package/gRPC).

For example, on Debian/Ubuntu:

``` sh
$ sudo apt-get install php5-dev php5-cli php-pear
$ sudo pecl install grpc
```

### gRPC Dependencies

To download the dependencies of the gRPC PHP library, run Composer:

``` sh
$ cd vitess
vitess$ curl -sS https://getcomposer.org/installer | php
vitess$ php composer.phar install
```

## Unit Tests

To run the tests, first install PHPUnit:

``` sh
$ wget https://phar.phpunit.de/phpunit-4.8.9.phar
$ mv phpunit-4.8.9.phar $VTROOT/bin/phpunit
$ chmod +x $VTROOT/bin/phpunit
```

Then run the tests like this:

``` sh
vitess$ . dev.env
vitess$ make php_test
```

### Coverage

In addition to PHPUnit, you also need to install xdebug, if you want to see
coverage:

``` sh
$ sudo pecl install xdebug
[...]
Build process completed successfully
Installing '/usr/lib/php5/20121212/xdebug.so'

# Where should we put the ini file?
$ php --ini
Configuration File (php.ini) Path: /etc/php5/cli
Loaded Configuration File:         /etc/php5/cli/php.ini
Scan for additional .ini files in: /etc/php5/cli/conf.d

# Make an ini file for xdebug.
$ sudo sh -c "echo \"zend_extension=$(pecl config-get ext_dir default)/xdebug.so\" > /etc/php5/cli/conf.d/20-xdebug.ini"

# Check that xdebug is being loaded.
$ php -m | grep xdebug
xdebug
```

Then you can run a coverage check with PHPUnit:

``` sh
vitess$ phpunit --coverage-html php/_test

# Open in browser.
vitess$ xdg-open php/_test/index.html
```


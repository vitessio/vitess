# Vitess PHP

This folder contains the PHP client for Vitess.

See `demo.php` for a simple example of using the API.

## Prerequisites

PHP 5.5+ is recommended.

This library uses the BSON codec library contained within the MongoDB plugin.
For example, you can install this on Debian/Ubuntu like this:

``` sh
$ sudo apt-get install php5-dev php5-cli php-pear
$ sudo pecl install mongo
```

## Unit Tests

To run the tests, first install PHPUnit:

``` sh
$ wget https://phar.phpunit.de/phpunit.phar
$ mv phpunit.phar $VTROOT/bin/phpunit
```

Then run the tests like this:

``` sh
vitess/php$ phpunit tests
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
$ sudo sh -c "echo 'zend_extension=xdebug.so' > /etc/php5/cli/conf.d/20-xdebug.ini"

# Check that xdebug is being loaded.
$ php -m | grep xdebug
xdebug
```

Then you can run a coverage check with PHPUnit:

``` sh
vitess/php$ phpunit --coverage-html _test tests

# Open in browser.
vitess/php$ xdg-open _test/index.html
```


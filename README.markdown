Vitess
======

Vitess is a set of servers and tools meant to facilitate scaling of MySQL databases for the web.
It's currently used as a fundamental component of YouTube's MySQL infrastructure.

The project is currently under active development. We haven't spent much time on documentation.
However, you can subscribe to our mailing list vitess@googlegroups.com for questions and updates.

Development
-----------

[Install Go](http://golang.org/doc/install).

``` sh
cd $WORKSPACE
sudo apt-get install automake libtool flex bison memcached python-mysqldb # to compile MySQL
git clone git@github.com:youtube/vitess.git src/github.com/youtube/vitess
cd src/github.com/youtube/vitess
./bootstrap.sh
. ./dev.env
```

Optionally:

``` sh
VTDATAROOT=... #  $VTROOT/vt if not set
VTPORTSTART=15000
```

To run the tests:

``` sh
make  # run the tests
```

License
-------

Unless otherwise noted, the vitess source files are distributed
under the BSD-style license found in the LICENSE file.

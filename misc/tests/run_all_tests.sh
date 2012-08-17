#!/bin/bash

# Run all the go unit tests found in the vitess sub-directories.
# memcached is required.

if [ ! -e /usr/bin/memcached ]; then
  echo "memcached is required to run the tests in ./go/memcache"
  echo "may have to run:"
  echo "  sudo apt-get install memcached"
  echo "  sudo service memcached stop"
  echo "  sudo update-rc.d -f memcached remove"
fi

for dir in `find . -name *_test.go | xargs -n 1 dirname | sort -u`; do
  echo "Running tests in $dir:"
  if [ $dir == "./go/logfile" -o \
       $dir == "./go/vt/client2" -o \
       $dir == "./go/vt/mysqlctl" -o \
       $dir == "./go/vt/wrangler" -o \
       $dir == "./go/zk" -o \
       $dir == "./go/zk/zkctl" ]; then
    echo "  skipping this one, it's broken now"
    echo
    continue
  fi
  (cd $dir && go test)
  result=$?
  if [ $result != 0 ]; then
    echo
    echo "Tests failed in $dir"
    exit 1
  fi
  echo
done

echo "All non-skipped tests passed!"

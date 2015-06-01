#! /bin/bash

set -e

if [ $MAKE_TARGET != "unit_test" ]; then
  make build
fi

make $MAKE_TARGET

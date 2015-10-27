#!/bin/bash

# This script generates PHP stubs for our protobufs.
# It should be run from $VTTOP.

# Dependencies:
#   - PHP 5.3+
#   - PEAR
#   - protoc-gen-php: http://www.grpc.io/docs/installation/php.html

set -e

# Set up and clean.
mkdir -p proto/build/proto2
mkdir -p php/src/proto
rm -f proto/build/proto2/*.proto
rm -f php/src/proto/*.php

# Translate proto3 to proto2.
pushd proto
for file in *.proto; do
  cat $file | ../tools/proto3to2.py > build/proto2/$file
done
popd

# Fix identifiers that are keywords in PHP.
pushd proto/build/proto2
sed -i -r \
  -e 's/\bUNSET\b/UNSET_/g' \
  -e 's/\bNULL\b/NULL_/g' \
   *.proto
popd

# Generate PHP.
for file in proto/build/proto2/*.proto; do
  protoc-gen-php -i proto/build/proto2 -o php/src/proto $file
done

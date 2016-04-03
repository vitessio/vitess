#!/bin/bash

# This script generates PHP stubs for our protobufs.
# It should be run from $VTTOP.

# Dependencies:
#   - PHP 5.5+
#   - PEAR
#   - gem (ruby)
#   - protoc (protobuf)
#   - protoc-gen-php:
#     https://github.com/grpc/grpc/tree/master/src/php#php-protobuf-compiler

set -e

# Set up and clean.
mkdir -p proto/build/proto2
mkdir -p php/src/Vitess/Proto
rm -f proto/build/proto2/*.proto
rm -rf php/src/Vitess/Proto/*

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
  protoc-gen-php -i proto/build/proto2 -o php/src $file
done

# Fix names of *Stub.php files (should be named after the *Client class).
for stubfile in `find php/src/Vitess/Proto -name '*Stub.php'`; do
  clientfile=${stubfile/Stub/Client}
  mv $stubfile $clientfile
done

# Strip dates from generated files.
for file in `find php/src -name '*.php'`; do
  sed -i -r '/^\/\/   Date: /d' $file
done

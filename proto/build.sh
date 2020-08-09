#!/bin/sh

for file in proto/*.proto
do
  name="$(basename "$file" | cut -d. -f1)"

  echo "Building $name.proto ..."
  dir="vt/proto/$name"
  mkdir -p "./$dir"

  protoc --go_out=plugins=grpc:. -Iproto "proto/$name.proto"
  mv "vitess.io/vitess/go/$dir/$name.pb.go" "./$dir/$name.pb.go"
  goimports -w "./$dir/$name.pb.go"
done

rm -rf vitess.io

#!/bin/bash
go_version=$(go version)
containsGo16=$(echo "$go_version" | grep -o "1.16")
if [ -z "$containsGo16" ]
then
  echo "setting up go"
  # changing directory so that installing and unzipping go does not overwrite the go directory in vitess from where it is called
  mkdir setupGo
  cd setupGo || exit
  wget https://dl.google.com/go/go1.16.4.linux-amd64.tar.gz
  sudo tar -xf go1.16.4.linux-amd64.tar.gz
  sudo rm -rf /usr/local/go
  sudo mv go /usr/local
  cd .. || exit
  sudo rm -rf setupGo
  go_version=$(go version)
  containsGo16=$(echo "$go_version" | grep -o "1.16")
  if [ -z "$containsGo16" ]
  then
    echo "could not install go"
    exit 1
  fi
else
    echo "go already present, no need to setup again"
fi
echo "$go_version"
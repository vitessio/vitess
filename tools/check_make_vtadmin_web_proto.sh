#!/bin/bash

source build.env

first_output=$(git status --porcelain)

make vtadmin_web_proto_types

second_output=$(git status --porcelain)

diff=$(diff <( echo "$first_output") <( echo "$second_output"))

if [[ "$diff" != "" ]]; then
  git diff
  echo "ERROR: Regenerated vtadmin web proto files do not match the current version."
  echo -e "List of files containing differences:\n$diff"
  exit 1
fi

echo "VTAdmin Web Protos are up to date"
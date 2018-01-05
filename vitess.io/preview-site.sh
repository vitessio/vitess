#!/bin/bash

use_docker=true
if [[ -n "$1" ]]; then
  if [[ "$1" == "--docker=false" ]]; then
    use_docker=false
  else
    echo "usage: ./preview-site.sh [--docker=false]"
    exit 1
  fi
fi

# Infer $VTTOP if it was not set.
if [[ -z "$VTTOP" ]]; then
  DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
  VTTOP="${DIR}/.."
fi

if [[ "$use_docker" == true ]]; then
  # Call this script from within the Docker container which has all dependencies installed.
  docker run -ti --rm -p 4000:4000 -v $VTTOP:/vttop -e VTTOP=/vttop vitess/publish-site /vttop/vitess.io/preview-site.sh --docker=false
  exit $?
fi

# Non-docker mode. Jekyll dependencies must be installed.
set -e

PREVIEW_DIR=$VTTOP/preview-vitess.io

rm -rf $PREVIEW_DIR
mkdir $PREVIEW_DIR

# launch web site locally
cd $VTTOP/vitess.io
bundle install
bundle exec jekyll serve --config _config_dev.yml --destination $PREVIEW_DIR

rm -rf $PREVIEW_DIR

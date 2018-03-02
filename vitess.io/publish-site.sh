#!/bin/bash

set -e

# This script runs Jekyll to translate our Markdown files at "doc/" into static web pages.
# The pages will be updated in "docs/" (note the extra s).
# The changes will go live on www.vitess.io when the commit is merged into the "master" branch.
#
# By default, the script runs Jekyll within Docker (recommended).
# Start it with --docker=false to run it directly instead.

use_docker=true
if [[ -n "$1" ]]; then
  if [[ "$1" == "--docker=false" ]]; then
    use_docker=false
  else
    echo "usage: ./publish-site.sh [--docker=false]"
    exit 1
  fi
fi

# Infer $VTTOP if it was not set.
if [[ -z "$VTTOP" ]]; then
  DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
  VTTOP="${DIR}/.."
fi

# Make sure we're within the Git repository before checking it.
pushd $VTTOP >/dev/null

# Output directory were the static web pages will be written to.
# Note: The directory is relative to $VTTOP.
website_dir="docs"
# Absolute path to website_dir.
website_path="${VTTOP}/${website_dir}"

# Compile to html pages.
if [[ "$use_docker" == true ]]; then
  docker run -ti --rm --name=vitess_publish_site -v $VTTOP:/vttop vitess/publish-site bash -c \
    "cd /vttop/vitess.io && \
     bundle install && \
     bundle exec jekyll build --destination /vttop/$website_dir && \
     chown -R $UID /vttop/vitess.io /vttop/$website_dir"
else
  (cd vitess.io && bundle install && bundle exec jekyll build --destination $website_path)
fi

# Change to docs/ directory.
pushd $website_path >/dev/null

# pre-commit checks
set +e
list=$(find . -name '*.html' ! -path '*/vendor/*' ! -path '*/web/*' | xargs grep -lE '^\s*([\-\*]|\d\.) ')
if [ -n "$list" ]; then
  echo
  echo "ERROR: The following pages appear to contain bulleted lists that weren't properly converted."
  echo "Make sure all bulleted lists have a blank line before them."
  echo
  echo "$list"
  exit 1
fi
set -e

popd >/dev/null # Leaving $website_path.
popd >/dev/null # Leaving $VTTOP.

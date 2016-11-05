#!/bin/bash

set -e

git checkout gh-pages

git rm --ignore-unmatch -rf *

git checkout master doc
git checkout master README.md
git checkout master index.md
git checkout master vitess.io

# compile to html pages
if [[ "$1" == "--docker" ]]; then
  docker run -ti --name=vitess_publish_site -v $PWD/vitess.io:/in vitess/publish-site bash -c \
    'cp -R /in /out && cd /out && bundle install && bundle exec jekyll build'
  docker cp vitess_publish_site:/out/. vitess.io/
  # There are cases where docker cp copies the contents of "out" in a
  # subdirectory "out" on the destination. If that happens, move/overwrite
  # everything up by one directory level.
  if [ -d vitess.io/out/ ]; then
    cp -a vitess.io/out/* vitess.io/
  fi
  docker rm vitess_publish_site
else
  (cd vitess.io && bundle install && bundle exec jekyll build)
fi

# clean up
rm -rf doc
rm -rf README.md

git add vitess.io/_site/*
git mv vitess.io/_site/* .
git add vitess.io/LICENSE
git mv vitess.io/LICENSE .

rm -rf vitess.io

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

git add -u

git commit -m "publish site `date`"

echo
echo "Please sanity-check the output: git diff HEAD~"
echo
echo "When you're ready to publish: git push origin gh-pages"

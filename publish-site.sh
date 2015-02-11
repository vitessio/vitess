#! /bin/bash 

set -e 

git checkout gh-pages

git rm --ignore-unmatch -rf *

git checkout master doc
git checkout master README.md
git checkout master vitess.io

mv doc vitess.io/_includes/
mv README.md vitess.io/_includes/

cd vitess.io
bundle install
bundle exec jekyll build
cd ..

git add vitess.io/_site/*
git mv vitess.io/_site/* .
git add vitess.io/LICENSE
git mv vitess.io/LICENSE .

rm -rf vitess.io
git add -u

git commit -m "publish site `date`"
git push origin gh-pages

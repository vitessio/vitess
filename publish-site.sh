#! /bin/bash 

set -e 

git checkout gh-pages

git rm --ignore-unmatch -rf *

git checkout master replace_doc_link.py
git checkout master doc
git checkout master README.md
git checkout master vitess.io

mkdir vitess.io/_includes/doc
mkdir -p vitess.io/_posts/doc

# create ref files for each doc
for d in doc/*.md
do
  name=${d:4}
  title=${name%%.*}
  docpath="vitess.io/_posts/doc/2015-01-01-${name}"
  touch $docpath
  echo "---" >> $docpath
  echo "layout: doc" >> $docpath
  echo "title: \"$title\"" >> $docpath
  echo "categories: doc" >> $docpath
  echo "toc: true" >> $docpath
  echo "---" >> $docpath
  echo "{% include $d %}" >> $docpath
done

# preserve links between docs
for d in `ls doc/*.md` README.md
do
  python replace_doc_link.py doc $d > vitess.io/_includes/$d
done

# compile to html pages
cd vitess.io
bundle install
bundle exec jekyll build
cd ..

# clean up
rm -rf doc
rm -rf README.md
rm replace_doc_link.py

git add vitess.io/_site/*
git mv vitess.io/_site/* .
git add vitess.io/LICENSE
git mv vitess.io/LICENSE .

rm -rf vitess.io

git add -u

git commit -m "publish site `date`"
git push origin gh-pages

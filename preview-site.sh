#! /bin/bash 

set -e 

PREVIEW_DIR=preview-vitess.io

rm -rf $PREVIEW_DIR
mkdir $PREVIEW_DIR
cp -rf vitess.io/* $PREVIEW_DIR

mkdir $PREVIEW_DIR/_includes/doc
mkdir -p $PREVIEW_DIR/_posts/doc

# create ref files for each doc
for d in doc/*.md
do
  name=${d:4}
  title=${name%%.*}
  docpath="$PREVIEW_DIR/_posts/doc/2015-01-01-${name}"
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
  python replace_doc_link.py doc $d > $PREVIEW_DIR/_includes/$d
done

# launch web site locally
cd $PREVIEW_DIR
bundle install
bundle exec jekyll serve --config _config_dev.yml
cd ..

rm -rf $PREVIEW_DIR

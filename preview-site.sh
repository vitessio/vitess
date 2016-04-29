#! /bin/bash 

if [[ "$1" == "--docker" ]]; then
  docker run -ti --rm -p 4000:4000 -v $VTTOP:/vttop -w /vttop vitess/publish-site /vttop/preview-site.sh
  exit $?
fi

set -e 

PREVIEW_DIR=preview-vitess.io

rm -rf $PREVIEW_DIR
mkdir $PREVIEW_DIR
cp -rf vitess.io/* $PREVIEW_DIR

mkdir $PREVIEW_DIR/_includes/doc
mkdir -p $PREVIEW_DIR/posts/
mkdir -p $PREVIEW_DIR/_posts/doc

# preserve links between docs
for d in `ls doc/*.md` README.md index.md
do
  python replace_doc_link.py doc $d > $PREVIEW_DIR/_includes/$d
done

# copy blog posts
cp doc/blog/index.md $PREVIEW_DIR/posts/
cp doc/blog/posts/* $PREVIEW_DIR/_posts/
cp doc/blog/posts/* $PREVIEW_DIR/posts/

# launch web site locally
cd $PREVIEW_DIR
bundle install
bundle exec jekyll serve --config _config_dev.yml
cd ..

rm -rf $PREVIEW_DIR

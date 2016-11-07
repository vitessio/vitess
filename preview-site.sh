#! /bin/bash 

if [[ "$1" == "--docker" ]]; then
  docker run -ti --rm -p 4000:4000 -v $VTTOP:/vttop -w /vttop vitess/publish-site /vttop/preview-site.sh
  exit $?
fi

set -e 

PREVIEW_DIR=preview-vitess.io

rm -rf $PREVIEW_DIR
mkdir $PREVIEW_DIR

# launch web site locally
cd vitess.io
bundle install
bundle exec jekyll serve --config _config_dev.yml --destination ../${PREVIEW_DIR}
cd ..

rm -rf $PREVIEW_DIR

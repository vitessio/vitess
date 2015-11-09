#!/bin/bash

# This script runs pylint with our desired flags.
# It's used by the pre-commit hook, but is a separate script
# so you can run it manually too.

PYLINT=/usr/bin/gpylint

file=$1

if [[ "$file" =~ \btest/ ]] ; then
  mode=style,test
else
  mode=style
fi

$PYLINT --mode $mode \
  --disable g-bad-file-header,g-bad-import-order,g-unknown-interpreter \
  --module-header-template '' \
  --msg-template '{path}:{line}:{msg_id}{obj_prefix}{obj}: {msg}{sym_separator}[{symbol}]' $file

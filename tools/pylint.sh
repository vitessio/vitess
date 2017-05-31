#!/bin/bash

# Copyright 2017 Google Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

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

# This script is a sample hook to test the plumbing

EXIT_CODE=0

echo "TABLET_ALIAS:" $TABLET_ALIAS

for arg in $@ ; do
  echo "PARAM:" $arg
  if [ "$arg" == "--to-stderr" ]; then
    echo "ERR:" $arg 1>&2
  fi
  if [ "$arg" == "--exit-error" ]; then
    EXIT_CODE=1
  fi
done

exit $EXIT_CODE

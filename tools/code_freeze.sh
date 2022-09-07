#!/bin/bash

# Copyright 2022 The Vitess Authors.
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

freeze=$1
branch=$2
code_freeze_workflow="./.github/workflows/code_freeze.yml"

if [ "$freeze" != "freeze" && "$freeze" != "unfreeze" ]; then
    echo "the first argument must be either 'freeze' or 'unfreeze'"
    exit 1
fi

git checkout -b $branch-code-freeze-1 $branch
for (( i = 2; $? != 0; i++ )); do
  git checkout -b $branch-code-freeze-$i $branch
done

if [ "$freeze" == "freeze" ]; then
    sed -i.bak -E "s/exit (.*)/exit 1/g" $code_freeze_workflow
elif [ "$freeze" == "unfreeze" ]; then
    sed -i.bak -E "s/exit (.*)/exit 0/g" $code_freeze_workflow
fi

rm -f $code_freeze_workflow.bak

git add --all
git commit -n -s -m "Code $freeze of $branch"

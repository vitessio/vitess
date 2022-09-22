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

#############
#
# This script changes the code_freeze workflow to either fail or succeed.
#
# The first argument of the script is a string, which should be either: "freeze" or "unfreeze".
#   - If the argument == "freeze" then the workflow will always fail.
#   - If the argument == "unfreeze" then the workflow will always succeed.
#
# The second argument is the name of the branch you want to freeze or unfreeze.
# The script takes care of creating a branch that is based on top of the branch
# you want to freeze/unfreeze. A commit is then created on this new branch, allowing
# you to push the commit and create a Pull Request that you merge against the input
# branch.
#
#############

freeze=$1
branch=$2
code_freeze_workflow="./.github/workflows/code_freeze.yml"

if [ "$freeze" != "freeze" && "$freeze" != "unfreeze" ]; then
    echo "the first argument must be either 'freeze' or 'unfreeze'"
    exit 1
fi

new_branch_name=$branch-code-freeze-1
git checkout -b $new_branch_name $branch
for (( i = 2; $? != 0; i++ )); do
  new_branch_name=$branch-code-freeze-$i
  git checkout -b $new_branch_name $branch
done

if [ "$freeze" == "freeze" ]; then
    sed -i.bak -E "s/exit (.*)/exit 1/g" $code_freeze_workflow
elif [ "$freeze" == "unfreeze" ]; then
    sed -i.bak -E "s/exit (.*)/exit 0/g" $code_freeze_workflow
fi

rm -f $code_freeze_workflow.bak

git add --all
git commit -n -s -m "Code $freeze of $branch"

echo " ------------------------------ "
echo " "
echo "The branch is ready to be pushed. Push it using the following line and create a PR against ${branch}"
echo "      git push upstream ${new_branch_name}"
echo " "
echo "(make sure to replace upstream with vitessio/vitess' remote)"

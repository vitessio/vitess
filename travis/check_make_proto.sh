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

# Verifies that the generated protobuf and gRPC files under version control
# are up to date.

# It does so by running "make proto" and verifying that any files tracked by
# Git do not change. If that's not the case, "make proto" must be run and all
# changed files must be included in the pull request.


# We do error checking manually.
set +e

function error() {
  script_name=`basename "${BASH_SOURCE[0]}"`
  echo
  echo -e "ERROR: $script_name: $1"
  exit 1
}

# Undo changes to vendor.json. govendor sometimes changes it, which is not necessary.
git checkout vendor/vendor.json

git diff --exit-code
if [ $? -ne 0 ]; then
  error "We cannot check if 'make proto' is up to date because some files have already changed. Please see the diff above and fix any local modifications happening prior to this step."
fi

make proto
if [ $? -ne 0 ]; then
  error "Failed to run 'make proto'. Please see the error above and fix it. This should not happen."
fi

# Check if the file only changed due to a different Go version.
# Go 1.7 has changes to the compression library which results in different
# output and therefore in different generated protobuf code.
git diff -U0 --no-prefix | grep -vE "(bytes of a gzipped FileDescriptorProto|[-\+]{3} .*\.go$|diff --git .*\.go$|^index |^@@ .+ @@ var fileDescriptor0 = \[\]byte\{$|^[-\+]	(0x[0-9a-f]{2}, )*0x[0-9a-f]{2},$)"
if [ $? -eq 1 ]; then
  # The protobuf files changed only due to a different Go version.
  # Revert the changes and continue with the actual check.
  git checkout .
fi

git diff --exit-code
if [ $? -ne 0 ]; then
  error "Generated protobuf and gRPC files are not up to date. See diff above. You have to generate them locally and include them in your pull request.\n\nTherefore run a) ./bootstrap.sh b) make proto and c) commit the changed files."
fi

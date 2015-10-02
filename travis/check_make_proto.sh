#!/bin/bash

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

git diff --exit-code
if [ $? -ne 0 ]; then
  error "We cannot check if 'make proto' is up to date because some files have already changed. Please see the diff above and fix any local modifications happening prior to this step."
fi

make proto
if [ $? -ne 0 ]; then
  error "Failed to run 'make proto'. Please see the error above and fix it. This should not happen."
fi

git diff --exit-code
if [ $? -ne 0 ]; then
  error "Generated protobuf and gRPC files are not up to date. See diff above. You have to generate them locally and include them in your pull request.\n\nTherefore run a) ./bootstrap.sh b) make proto and c) commit the changed files."
fi

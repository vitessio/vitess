#!/bin/bash

# Runs all the hooks in misc/git/hooks against the specified base branch
# (default origin/master), and exits if any of them fail.
set -e

: ${GIT_BASE:="origin/master"}
export GIT_BASE

# This is necessary because the Emacs extensions don't set GIT_DIR.
if [ -z "$GIT_DIR" ]; then
  DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
  GIT_DIR="${DIR}/.."
fi
for hook in $GIT_DIR/misc/git/hooks/*; do
  $hook
done

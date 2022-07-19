#!/usr/bin/env bash

set -euo pipefail

snapshot="snapshot-$(date '+%Y-%m-%d-%H-%M-%S')"

# Copy current latest branch
git checkout latest
git checkout -b "${snapshot}"

# Rebase from upstream master
git checkout latest
git pull --rebase upstream main

# Exit if nothing has changed
git diff --cached --quiet "${snapshot}" && exit 0

# Push everything
git push --set-upstream origin "${snapshot}"
git push --force origin latest

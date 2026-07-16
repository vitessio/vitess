#!/bin/bash

# Copyright 2026 The Vitess Authors.
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

# This script checks that every tracked Go file carries a license header.
# Generated files (marked with "DO NOT EDIT") are skipped, since the
# code generators decide whether to emit a header.
#
# It uses github.com/google/addlicense, which accepts any existing
# copyright notice; files derived from third-party code keep their
# original attribution.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]:-$0}")/.."

files=()
while IFS= read -r -d '' file; do
  if head -25 "$file" | grep -q "DO NOT EDIT"; then
    continue
  fi
  files+=("$file")
done < <(git ls-files -z -- '*.go')

if ! go tool -modfile=tools/addlicense/go.mod addlicense -check "${files[@]}"; then
  echo
  echo "The Go files listed above are missing a license header."
  echo "Please add the standard Vitess license header, e.g. by running:"
  echo "  go tool -modfile=tools/addlicense/go.mod addlicense -c 'The Vitess Authors.' <file>"
  exit 1
fi

echo "All Go files have license headers."

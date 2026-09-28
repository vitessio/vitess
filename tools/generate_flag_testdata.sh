#!/bin/bash

set -euo pipefail

DIR="go/flags/endtoend"

templatize_help_text() {
    $1 --help 2>&1 | \
        sed -e 's/{{/{{ "{{/' \
            -e 's/}}/}}" }}/' \
            -e  's/Paths to search for config files in. (default .*)/Paths to search for config files in. (default [{{ .Workdir }}])/'
    return 0
}

export -f templatize_help_text

find "${DIR}" -iname '*.txt' \
  | xargs -n1 -P"$(nproc)" bash -c '
    testfile="$0"
    base="${testfile##*/}"
    binary="${base%.*}"
    templatize_help_text "$binary" > "$testfile"
    echo "done: $testfile"
  '

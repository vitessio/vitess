#!/bin/bash

set -euo pipefail
templatize_help_text() {
    $1 --help 2>&1 | \
        sed -e 's/{{/{{ "{{/' \
            -e 's/}}/}}" }}/' \
            -e  's/Paths to search for config files in. (default .*)/Paths to search for config files in. (default [{{ .Workdir }}])/'
    return 0
}

export -f templatize_help_text

find go/flags/endtoend -iname '*.txt' | \
    xargs basename | \
    cut -d. -f1 | \
    xargs -I{} \
        bash -c 'templatize_help_text "{}" >go/flags/endtoend/{}.txt'
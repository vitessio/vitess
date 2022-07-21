#!/usr/bin/env bash
#/ Usage: fmt [--check]
#/ --check Fail if changed files need changes, do not format in place.

set -eu

usage() {
    grep '^#/' "$0" | cut -c'4-' >&2
    exit "$1"
}

if ! [ -x "$(command -v jsonnet)" ] && ! [ -x "$(command -v jsonnetfmt)" ]; then
  echo 'jsonnet or jsonnetfmt executable not found'
  exit 1
fi

if [ -x "$(command -v jsonnetfmt)" ]; then
    JSONNET_COMMAND='jsonnetfmt'
else
    JSONNET_COMMAND='jsonnet fmt'
fi

CHECK="false"
EXIT_STATUS=0
FILES_CHANGED="$(git diff --name-only origin/main --diff-filter=d | grep -E '(jsonnet|TEMPLATE|libsonnet)$' || true)"

while [ "$#" -gt 0 ]; do
    case "$1" in
        -c|--check) CHECK="true"; shift;;
        *) usage ;;
    esac
done

if [ "${CHECK}" == "true" ]; then
    if [ -n "${FILES_CHANGED}" ]; then
        for FILE in ${FILES_CHANGED}; do
            set +e
            echo -n "Checking $FILE: "
            ${JSONNET_COMMAND} --test "${FILE}"
            EC=$?
            if [ ${EC} -ne 0 ]; then
                echo "⚠️"
                EXIT_STATUS=1
            else
                echo "✅"
            fi
            set -e
        done
        echo ""
    fi

    echo -n "STATUS:"
    if [ "$EXIT_STATUS" -eq 0 ]; then
         echo "✅"
    else
        echo "❌"
    fi

    exit $EXIT_STATUS

else
    for FILE in $FILES_CHANGED; do
        echo -n "Formatting $FILE: "
        $JSONNET_COMMAND -n 2 --max-blank-lines 2 --string-style s --comment-style s -i ../$FILE
        echo "✅"
    done
fi

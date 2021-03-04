#!/bin/bash

find . -type f | grep -v '^./.git' | while read -r f ; do git check-ignore "$f" || echo "$f" ; done | while read -r f ; do
  # A file not ignored by .gitignore. Let's check if it's covered by CODEOWNERS
  # Get rid of leading dot (e.g. './web/vtctld2/src/app/index.ts' => '/web/vtctld2/src/app/index.ts')
  f="${f#"."}"
  grep -E "^/" .github/CODEOWNERS | awk '{print $1}' | while read -r codeowner_path ; do
    case "$f" in
      $codeowner_path) exit 3 ;;   # Perfect match. All good.
      $codeowner_path/*) exit 3 ;; # Prefix (path) match. All good.
      *) ;;                        # No match: keep looping on next codeowner path
    esac
  done
  if [ "$?" != "3" ] ; then
    # No indication for CODEOWNER match for file $f
    echo "$f"
  fi
done

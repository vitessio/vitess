#!/bin/bash

find . -type f | grep -v '^./.git' | while read -r f ; do git check-ignore "$f" || echo "$f" ; done | while read -r f ; do
  # A file not ignored by .gitignore. Let's check if it's covered by CODEOWNERS
  f="${f#"."}"
  grep -E "^/" .github/CODEOWNERS | awk '{print $1}' | while read -r p ; do
    case "$f" in
      $p) ;;       # Perfect math. All good.
      $p/*) ;;     # Prefix (path) match. All good.
      *) exit 1 ;; # No match: an error
    esac
  done && exit 0
  # If we're stil here, then that means no CODEOWNER was found for file $f
  echo "$f" 
done > /dev/null 

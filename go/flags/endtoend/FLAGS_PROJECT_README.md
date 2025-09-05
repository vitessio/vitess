Note: This document will be deleted once the flags migration project is done. It is intended as context for Claude to
help with the tasks related to this project.

## Project Description

What we are doing as part of this project is to replace all underscores (_)s in the flag names to dashes (-)s. We
need to be backwardly compatible with previous versions and support both versions in our apps.

The following files are useful for this project:

* go/vt/utils/flags.go: This contains utility functions for working with flags during the flags refactor project.
* go/flags/endtoend/count_flags.sh: for counting pending flags to be migrated in the binaries whose help text is
  present in the same directory as this file.

You can find some PRs that have done this in the list
from: https://github.com/vitessio/vitess/pulls?q=is%3Apr+flags+author%3Amounicasruthi+is%3Amerged
For example: https://github.com/vitessio/vitess/pull/17975, https://github.com/vitessio/vitess/pull/18296

## Miscellaneous notes

* We don't want to extend the usage of DualFormatVar for flags, because technically that supports a mix of
  dashes and underscores, and that is not what we want to do.

* The binaries we are interested in:
    * vtctld
    * vtctldclient
    * vttablet
    * vtgate
    * vtorc
    * mysqlctl
    * mysqlctld
    * vtcombo
    * vttestserver
    * vtbackup
    * vtclient
    * vtbench
 
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


## Pending Tasks

1. Find all the flags per binary by writing a similar script like count_flags.sh that 
    * per binary, lists all flags for tha binary
    * per flag, lists all binaries for that flag
    * count of all binaries pending with number of flags pending for each
2. Group flags by function 
3. Replace the flags across the code base using the flags utility functions from go/vt/utils/flags.go as done in 
   previous PRs
4. Ensure unit tests run in the folders where the files are being modified
5. Pause after each group for manual validation and check-in and potentially separate PR creation
6. Repeat Steps 3 to 5 until all flags are replaced
7. Finally, go through the code base looking for any flags with underscores still being used and see how we can 
   replace that with the dashed version. For this, since there are several identifiers using underscores, we should
   do a grep for the underscore versions of the all the flags either one by one or with a giant regexp, which ever 
   works best. Or we could also create a golang program that goes through every go file in the repository. Whichever 
   is the best approach, we are ok running these programs for long durations, say overnight, if required on our 
   local machines.

## Miscellaneous notes

* We don't want to extend the usage of DualFormatVar for flags, because technically that supports a mix of
  dashes and underscores, and that is not what we want to do.
* Sometimes tests fail due to upgrade/downgrade and we need to use the `GetFlagVariantForTestsByVersion` function 
  but that needs to be manually determined from the failures in the CI upgrade/downgrade tests. If you see this 
  function being used DO NOT replace it.

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
 
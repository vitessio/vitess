This update fixes several regressions that were deemed significant enough to be backported to the release branch. In addition, the experimental feature to set system variables is now behind a flag that disables it by default.

Configuration Changes

Bugs Fixed
* Fix where clause in information schema with correct database name #6599

Enhancements 
* Make emergency reparents more robust. #6206

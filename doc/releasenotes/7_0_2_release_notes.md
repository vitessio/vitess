This update fixes several regressions that were deemed significant enough to be backported to the release branch. In addition, the experimental feature to set system variables is now behind a flag that disables it by default.

Configuration Changes

Bugs Fixed
* Backport restore: do not change tablet type to RESTORE if not actually performing a restore #6687
* Backport: tm init: publish displayState #6686
* Backport: tm: add more logging to checkMastership #6685
* Backport: tm: fix replmanager deadlock #6634

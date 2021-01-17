This update fixes several regressions that were deemed significant enough to be backported to the release branch. 

## Bugs Fixed

* vtgate : Operator precedence must take associativity into consideration #6764
* vtgate: Fix reserved connection in autocommit mode on DML #6748
* vDiff: fix panic for tables with a unicode_loose_md5 vindex #6745
* vttablet: Turn off schema tracker by default #6746
* restore: Do not change tablet type to RESTORE if not actually performing a restore #6687
* vttablet : Tablet manager init should publish displayState #6686
* vttablet: Tablet manager add more logging to checkMastership #6685
* vttablet : Fix replmanager deadlock #6634

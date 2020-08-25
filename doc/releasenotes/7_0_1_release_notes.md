This update fixes several regressions that were deemed significant enough to be backported to the release branch.
In addition, the experimental feature to set system variables is now behind a flag that disables it by default.

## Configuration Changes
* Vtgate: added flag enable_system_settings to enable passthrough system settings, defaults to false #6562

## Bugs Fixed
* Vreplication: Materialize should only get schema from source tablets if target is missing tables #6623
* Vtctld UI: mysql port is not displayed in tablet list #6597 #6600
* Vtgate: transaction killer was closing reserved connection after the specified time. Only idle connections should be closed. Idle transactions should be rolled back. #6583
* Vtcompose: docker-compose example was broken by tabletmanager changes #6587
* Vreplication: tablet_picker should respect canceled context. Also, keep trying to find a tablet until context expires. #6567
* Vtgate: vindex update/delete by destination #6541 #6572
* Vtgate: some usages of EXPLAIN were broken #6574 #6581
* Vtgate: handle multiple settings in single SET statement #6504
* Vtgate: cleanup shard session for reserved connection on failure #6563
* Vtgate: if executing DDL on reserved connection, start transaction if needed #6555
* Vtgate: make sure lookup vindexes use same transaction if one is open #6505
* Healthcheck: add diagnostic logging, do not add tablet if it is shutting down #6512 

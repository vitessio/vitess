This release complies with VEP-3 which removes the upgrade order requirement. Components can be upgraded in any order. It is recommended that the upgrade order should still be followed if possible, except to canary test the new version of VTGate before upgrading the rest of the components.

## Bug fixes
### Cluster management
 * Backport: Respect -disable_active_reparents in backup/restore #8063
### Other
 * [9.0] fix regression - should be able to plan subquery on top of subquery #7683
### Query Serving
 * [9.0] Fix for reserved connection usage with transaction #7666
 * [9.0] Fix MySQL Workbench failure on login with `select current_user()` #7706
 * [9.0] make sure to handle subqueries on top of subqueries #7776
 * [9.0] make sure to not log sensitive information #7778
 * [9.0] ddl bypass planner #8035
 * [9.0] Memory Sort to close the goroutines when callback returns error #8040
 * [9.0] Fix bug with reserved connections to stale tablets #8041
 * [9.0] Fix for Query Serving when Toposerver is Down #8046
 * ignore the error and log as warn if not able to validate the current system settings value #8062
### VReplication
 * VReplication: fix vreplication timing metrics #8060
 * VReplication: Pad binlog values for binary() columns to match the value returned by mysql selects #8061
## Documentation
### Other
 * 9.0.0 Release Notes #7384
## Other
### Build/CI
 * Fix Dockerfiles for vtexplain and vtctlclient #7418
### Query Serving
 * Fix table parsing on VSchema generation #7511
 * [9.0] Show anywhere plan fix to consider default keyspace #7530
 * [9.0] Reset Session for Reserved Connection when the connection id is not found #7544
 * Healthcheck: update healthy tablets correctly when a stream returns an error or times out #7732
### VReplication
 * MoveTables: Refresh SrvVSchema and source tablets on completion #8059


The release includes 43 commits (excluding merges)
Thanks to all our contributors: @askdba, @deepthi, @dyv, @harshit-gangal, @rafael, @rohit-nayak-ps, @shlomi-noach, @systay


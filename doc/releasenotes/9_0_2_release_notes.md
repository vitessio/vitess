## Bug fixes
### Cluster management
 * Restore: Check disable_active_reparents properly before waiting for position update #8114
### Query Serving
 * Fix information_schema query with system schema in table_schema filter #8095
 * Fix for issue with information_schema queries with both table name and schema name predicates #8096
 * Fix for transactions not allowed to finish during PlannedReparentShard #8098
 * PRIMARY in index hint list #8158
## CI/Build
### Build/CI
 * Release 9.0.1 #8065
 * 9.0.0: update release notes with known issue #8080 #8082
 * Added release script to the makefile #8182
## Performance
### Cluster management
 * Revert "backup: Use pargzip instead of pgzip for compression." #8174
The release includes 16 commits (excluding merges)
Thanks to all our contributors: @GuptaManan100, @deepthi, @harshit-gangal, @rafael, @systay

# Changelog of Vitess v15.0.0

### Bug fixes 
#### Query Serving
 * fix: scalar aggregation engine primitive #10465
 * fix: aggregation empty row on join with grouping and aggregations #10480
### CI/Build 
#### Governance
 * Update the comment for review checklist with an item for CI workflows #10471
### Documentation 
#### CLI
 * [vtctldclient] Update CLI docs for usages, flags, and aliases #10502 
#### VTAdmin
 * [vtadmin] Document known issue with node versions 17+ #10483
### Enhancement 
#### Build/CI
 * Add name to static check workflow #10470 
#### Query Serving
 * Refactor aggregation AST structs #10347
 * fix: change planner_version to planner-version everywhere #10453
 * Add support for alter table rename column #10469
 * schemadiff: `ColumnRenameStrategy` in DiffHints #10472
 * Add parsing support for performance schema functions #10478
 * schemadiff: TableRenameStrategy in DiffHints #10479
 * OnlineDDL executor: adding log entries #10482
### Internal Cleanup 
#### General
 * Remove v2 resharding fields #10409 
#### Query Serving
 * Reduce shift-reduce conflicts #10500
 * feat: don't stop if compilation errors are happening on the generated files #10506 
#### VTAdmin
 * [vtadmin] Rename ERS/PRS pools+flags properly #10460 
#### web UI
 * Remove sharding_column_name and sharding_column_type from vtctld2 #10459
### Release 
#### General
 * Post release `v14.0.0-RC1` steps #10458
### Testing 
#### Build/CI
 * test: reduce number of vttablets to start in the tests #10491 
#### VTAdmin
 * [vtadmin] authz tests - tablet actions #10457
 * [vtadmin] Add authz tests for remaining non-schema related actions #10481
 * [vtadmin] Add schema-related authz tests #10486


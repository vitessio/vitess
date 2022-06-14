# Changelog of Vitess v15.0.0

### Bug fixes 
#### Query Serving
 * fix: scalar aggregation engine primitive #10465
 * fix: aggregation empty row on join with grouping and aggregations #10480
### Documentation 
#### VTAdmin
 * [vtadmin] Document known issue with node versions 17+ #10483
### Enhancement 
#### Query Serving
 * fix: change planner_version to planner-version everywhere #10453
 * Add support for alter table rename column #10469
 * schemadiff: `ColumnRenameStrategy` in DiffHints #10472
 * Add parsing support for performance schema functions #10478
 * schemadiff: TableRenameStrategy in DiffHints #10479
 * OnlineDDL executor: adding log entries #10482
### Internal Cleanup 
#### General
 * Remove v2 resharding fields #10409 
#### VTAdmin
 * [vtadmin] Rename ERS/PRS pools+flags properly #10460 
#### web UI
 * Remove sharding_column_name and sharding_column_type from vtctld2 #10459
### Testing 
#### VTAdmin
 * [vtadmin] authz tests - tablet actions #10457
 * [vtadmin] Add authz tests for remaining non-schema related actions #10481
 * [vtadmin] Add schema-related authz tests #10486


The release includes 21 commits (excluding merges)

Thanks to all our contributors: @GuptaManan100, @K-Kumar-01, @ajm188, @arvind-murty, @dbussink, @frouioui, @harshit-gangal, @notfelineit, @rohit-nayak-ps, @rsajwani, @shlomi-noach, @systay


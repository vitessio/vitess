/*
Copyright 2022 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vdiff

const (
	sqlAnalyzeTable = "analyze table `%s`.`%s`"
	sqlNewVDiff     = "insert into _vt.vdiff(keyspace, workflow, state, options, shard, db_name, vdiff_uuid) values(%a, %a, %a, %a, %a, %a, %a)"
	sqlResumeVDiff  = `update _vt.vdiff as vd, _vt.vdiff_table as vdt set vd.started_at = NULL, vd.completed_at = NULL, vd.state = 'pending',
					vdt.state = 'pending' where vd.vdiff_uuid = %a and vd.id = vdt.vdiff_id and vd.state in ('completed', 'stopped')
					and vdt.state in ('completed', 'stopped')`
	sqlRetryVDiff = `update _vt.vdiff as vd left join _vt.vdiff_table as vdt on (vd.id = vdt.vdiff_id) set vd.state = 'pending',
					vd.last_error = '', vdt.state = 'pending' where vd.id = %a and (vd.state = 'error' or vdt.state = 'error')`
	sqlGetVDiffByKeyspaceWorkflowUUID       = "select * from _vt.vdiff where keyspace = %a and workflow = %a and vdiff_uuid = %a"
	sqlGetMostRecentVDiffByKeyspaceWorkflow = "select * from _vt.vdiff where keyspace = %a and workflow = %a order by id desc limit %a"
	sqlGetVDiffByID                         = "select * from _vt.vdiff where id = %a"
	sqlDeleteVDiffs                         = `delete from vd, vdt, vdl using _vt.vdiff as vd left join _vt.vdiff_table as vdt on (vd.id = vdt.vdiff_id)
										left join _vt.vdiff_log as vdl on (vd.id = vdl.vdiff_id)
										where vd.keyspace = %a and vd.workflow = %a`
	sqlDeleteVDiffByUUID = `delete from vd, vdt using _vt.vdiff as vd left join _vt.vdiff_table as vdt on (vd.id = vdt.vdiff_id)
							where vd.vdiff_uuid = %a`
	sqlVDiffSummary = `select vd.state as vdiff_state, vd.last_error as last_error, vdt.table_name as table_name,
						vd.vdiff_uuid as 'uuid', vdt.state as table_state, vdt.table_rows as table_rows,
						vd.started_at as started_at, vdt.rows_compared as rows_compared, vd.completed_at as completed_at,
						IF(vdt.mismatch = 1, 1, 0) as has_mismatch, vdt.report as report
						from _vt.vdiff as vd left join _vt.vdiff_table as vdt on (vd.id = vdt.vdiff_id)
						where vd.id = %a order by table_name`
	// sqlUpdateVDiffState has a penultimate placeholder for any additional columns you want to update, e.g. `, foo = 1`.
	// It also truncates the error if needed to ensure that we can save the state when the error text is very long.
	sqlUpdateVDiffState   = "update _vt.vdiff set state = %s, last_error = left(%s, 1024) %s where id = %d"
	sqlUpdateVDiffStopped = `update _vt.vdiff as vd, _vt.vdiff_table as vdt set vd.state = 'stopped', vdt.state = 'stopped', vd.last_error = ''
							where vd.id = vdt.vdiff_id and vd.id = %a and vd.state != 'completed'`
	sqlGetVReplicationEntry          = "select * from _vt.vreplication %s"                            // A filter/where is added by the caller
	sqlGetVDiffsToRun                = "select * from _vt.vdiff where state in ('started','pending')" // what VDiffs have not been stopped or completed
	sqlGetVDiffsToRetry              = "select * from _vt.vdiff where state = 'error' and json_unquote(json_extract(options, '$.core_options.auto_retry')) = 'true'"
	sqlGetVDiffID                    = "select id as id from _vt.vdiff where vdiff_uuid = %a"
	sqlGetVDiffIDsByKeyspaceWorkflow = "select id as id from _vt.vdiff where keyspace = %a and workflow = %a"
	sqlGetTableRows                  = "select table_rows as table_rows from INFORMATION_SCHEMA.TABLES where table_schema = %a and table_name = %a"
	sqlGetAllTableRows               = "select table_name as table_name, table_rows as table_rows from INFORMATION_SCHEMA.TABLES where table_schema = %s and table_name in (%s) order by table_name"

	sqlNewVDiffTable = "insert into _vt.vdiff_table(vdiff_id, table_name, state, table_rows) values(%a, %a, 'pending', %a)"
	sqlGetVDiffTable = `select vdt.lastpk as lastpk, vdt.mismatch as mismatch, vdt.report as report
						from _vt.vdiff as vd inner join _vt.vdiff_table as vdt on (vd.id = vdt.vdiff_id)
						where vdt.vdiff_id = %a and vdt.table_name = %a`
	sqlUpdateTableRows           = "update _vt.vdiff_table set table_rows = %a where vdiff_id = %a and table_name = %a"
	sqlUpdateTableProgress       = "update _vt.vdiff_table set rows_compared = %a, lastpk = %a, report = %a where vdiff_id = %a and table_name = %a"
	sqlUpdateTableNoProgress     = "update _vt.vdiff_table set rows_compared = %a, report = %a where vdiff_id = %a and table_name = %a"
	sqlUpdateTableState          = "update _vt.vdiff_table set state = %a where vdiff_id = %a and table_name = %a"
	sqlUpdateTableStateAndReport = "update _vt.vdiff_table set state = %a, rows_compared = %a, report = %a where vdiff_id = %a and table_name = %a"
	sqlUpdateTableMismatch       = "update _vt.vdiff_table set mismatch = true where vdiff_id = %a and table_name = %a"

	sqlGetIncompleteTables = "select table_name as table_name from _vt.vdiff_table where vdiff_id = %a and state != 'completed' order by table_name"
)

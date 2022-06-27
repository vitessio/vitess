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

import "vitess.io/vitess/go/vt/withddl"

var withDDL *withddl.WithDDL

func init() {
	var ddls []string
	// Initial VDiff related schema
	ddls = append(ddls, sqlCreateSidecarDB, sqlCreateVDiffTable, sqlCreateVDiffTableTable, sqlCreateVDiffLogTable)
	// Changes to VDiff related schema over time
	ddls = append(ddls,
		"ALTER TABLE _vt.vdiff MODIFY COLUMN id bigint AUTO_INCREMENT",
		"ALTER TABLE _vt.vdiff CHANGE started_timestamp started_at timestamp NULL DEFAULT NULL",
		"ALTER TABLE _vt.vdiff CHANGE completed_timestamp completed_at timestamp NULL DEFAULT NULL",
		"ALTER TABLE _vt.vdiff MODIFY COLUMN state varbinary(64)",
		"ALTER TABLE _vt.vdiff_table MODIFY COLUMN table_name varbinary(128)",
		"ALTER TABLE _vt.vdiff_table MODIFY COLUMN state varbinary(64)",
		"ALTER TABLE _vt.vdiff_table MODIFY COLUMN lastpk varbinary(2000)",
	)
	withDDL = withddl.New(ddls)
}

const (
	sqlCreateSidecarDB  = "CREATE DATABASE IF NOT EXISTS _vt"
	sqlCreateVDiffTable = `CREATE TABLE IF NOT EXISTS _vt.vdiff (
		id int AUTO_INCREMENT,
		vdiff_uuid varchar(64) NOT NULL,
		workflow varbinary(1024),
		keyspace varbinary(1024),
		shard varchar(255) NOT NULL,
		db_name varbinary(1024),
		state varbinary(1024),
		options json,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		started_timestamp timestamp NULL DEFAULT NULL,
		liveness_timestamp timestamp NULL DEFAULT NULL,
		completed_timestamp timestamp NULL DEFAULT NULL,
		unique key uuid_idx (vdiff_uuid),
		primary key (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

	sqlCreateVDiffTableTable = `CREATE TABLE IF NOT EXISTS _vt.vdiff_table(
		vdiff_id varchar(64) NOT NULL,
		table_name varbinary(1024),
		state varbinary(128),
		lastpk varbinary(1024),
		table_rows int not null default 0,
		rows_compared int not null default 0,
		mismatch bool not null default false,
		report json,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		primary key (vdiff_id, table_name)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

	sqlCreateVDiffLogTable = `CREATE TABLE IF NOT EXISTS _vt.vdiff_log (
		id int AUTO_INCREMENT,
		vdiff_id int not null,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		message text NOT NULL,
		primary key (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

	sqlNewVDiff    = "insert into _vt.vdiff(keyspace, workflow, state, options, shard, db_name, vdiff_uuid) values(%s, %s, '%s', %s, '%s', '%s', '%s')"
	sqlResumeVDiff = `update _vt.vdiff as vd, _vt.vdiff_table as vdt set vd.started_at = NULL, vd.completed_at = NULL, vd.state = 'pending',
						vd.options = %s, vdt.state = 'pending', vdt.rows_compared = 0 where vd.vdiff_uuid = %s and vd.id = vdt.vdiff_id`
	sqlGetVDiffByKeyspaceWorkflowUUID = "select * from _vt.vdiff where keyspace = %s and workflow = %s and vdiff_uuid = %s"
	sqlGetMostRecentVDiff             = "select * from _vt.vdiff where keyspace = %s and workflow = %s order by id desc limit 1"
	sqlGetVDiffByID                   = "select * from _vt.vdiff where id = %d"
	sqlVDiffSummary                   = `select vd.state as vdiff_state, vdt.table_name as table_name,
										vd.vdiff_uuid as 'uuid', vdt.state as table_state, vdt.table_rows as table_rows,
										vd.started_at as started_at, vdt.rows_compared as rows_compared, vd.completed_at as completed_at,
										IF(vdt.mismatch = 1, 1, 0) as has_mismatch, vdt.report as report
										from _vt.vdiff as vd inner join _vt.vdiff_table as vdt on (vd.id = vdt.vdiff_id)
										where vdt.vdiff_id = %d`
	// sqlUpdateVDiffState has a penultimate placeholder for any additional columns you want to update, e.g. `, foo = 1`
	sqlUpdateVDiffState     = "update _vt.vdiff set state = %s %s where id = %d"
	sqlGetVReplicationEntry = "select * from _vt.vreplication %s"
	sqlGetPendingVDiffs     = "select * from _vt.vdiff where state = 'pending'"
	sqlGetVDiffID           = "select id as id from _vt.vdiff where vdiff_uuid = %s"
	sqlGetAllVDiffs         = "select * from _vt.vdiff order by id desc"

	sqlNewVDiffTable = "insert into _vt.vdiff_table(vdiff_id, table_name, state, table_rows) values(%d, %s, 'pending', %d)"
	sqlGetVDiffTable = `select vdt.lastpk as lastpk from _vt.vdiff as vd inner join _vt.vdiff_table as vdt on (vd.id = vdt.vdiff_id)
						where vdt.vdiff_id = %d and vdt.table_name = %s`
	sqlUpdateTableRows       = "update _vt.vdiff_table set table_rows = %d where vdiff_id = %d and table_name = %s"
	sqlUpdateTableProgress   = "update _vt.vdiff_table set rows_compared = %d, lastpk = %s where vdiff_id = %d and table_name = %s"
	sqlUpdateTableNoProgress = "update _vt.vdiff_table set rows_compared = %d where vdiff_id = %d and table_name = %s"
	sqlUpdateTableState      = "update _vt.vdiff_table set state = %s, report = %s where vdiff_id = %d and table_name = %s"
	sqlUpdateTableMismatch   = "update _vt.vdiff_table set mismatch = true where vdiff_id = %d and table_name = %s"

	sqlGetIncompleteTables = "select table_name as table_name from _vt.vdiff_table where vdiff_id = %d and state != 'completed'"
)

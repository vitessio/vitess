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
	ddls = append(ddls, sqlCreateSidecarDB, sqlCreateVDiffTable, sqlCreateVDiffTableTable, sqlCreateVDiffLogTable)
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

	sqlNewVDiff                       = "insert into _vt.vdiff(keyspace, workflow, state, options, shard, db_name, vdiff_uuid) values(%s, %s, '%s', %s, '%s', '%s', '%s')"
	sqlGetVDiffByKeyspaceWorkflowUUID = "select * from _vt.vdiff where keyspace = %s and workflow = %s and vdiff_uuid = %s"
	sqlGetMostRecentVDiff             = "select * from _vt.vdiff where keyspace = %s and workflow = %s order by id desc limit 1"
	sqlGetVDiffByID                   = "select * from _vt.vdiff where id = %d"
	sqlVDiffSummary                   = `select vt.vdiff_id, v.state as vdiff_state, vt.table_name, 
										v.vdiff_uuid as 'uuid',
										vt.state as table_state, vt.table_rows, 
										vt.rows_compared, 
										IF(vt.mismatch = 1, 1, 0) as has_mismatch, vt.report
										from _vt.vdiff v, _vt.vdiff_table vt 
										where v.id = vt.vdiff_id and v.id = %d`
	sqlUpdateVDiffState     = "update _vt.vdiff set state = %s where id = %d"
	sqlGetVReplicationEntry = "select * from _vt.vreplication %s"
	sqlGetPendingVDiffs     = "select * from _vt.vdiff where state = 'pending'"
	sqlGetAllVDiffs         = "select * from _vt.vdiff order by id desc"

	sqlNewVDiffTable       = "insert into _vt.vdiff_table(vdiff_id, table_name, state, table_rows) values(%d, %s, 'pending', %d)"
	sqlUpdateRowsCompared  = "update _vt.vdiff_table set rows_compared = %d where vdiff_id = %d and table_name = %s"
	sqlUpdateTableState    = "update _vt.vdiff_table set state = %s, report = %s where vdiff_id = %d and table_name = %s"
	sqlUpdateTableMismatch = "update _vt.vdiff_table set mismatch = true where vdiff_id = %d and table_name = %s"

	sqlGetIncompleteTables = "select * from _vt.vdiff_table where vdiff_id = %d and state != 'completed'"
)

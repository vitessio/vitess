/*
Copyright 2019 The Vitess Authors.

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

package onlineddl

import (
	"fmt"

	"vitess.io/vitess/go/vt/withddl"
)

const (
	sqlCreateSidecarDB             = "create database if not exists %s"
	sqlCreateSchemaMigrationsTable = `CREATE TABLE IF NOT EXISTS %s.schema_migrations (
		id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
		migration_uuid varchar(64) NOT NULL,
		keyspace varchar(256) NOT NULL,
		shard varchar(256) NOT NULL,
		mysql_table varchar(128) NOT NULL,
		migration_statement text NOT NULL,
		strategy varchar(128) NOT NULL,
		added_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		ready_timestamp timestamp NULL DEFAULT NULL,
		assigned_timestamp timestamp NULL DEFAULT NULL,
		started_timestamp timestamp NULL DEFAULT NULL,
		liveness_timestamp timestamp NULL DEFAULT NULL,
		completed_timestamp timestamp NULL DEFAULT NULL,
		migration_status varchar(128) NOT NULL,
		PRIMARY KEY (id),
		KEY uuid_idx (migration_uuid),
		KEY keyspace_shard_idx (keyspace,shard),
		KEY status_idx (migration_status, liveness_timestamp)
	) engine=InnoDB DEFAULT CHARSET=utf8mb4`
	sqlValidationQuery = `select 1 from schema_migrations limit 1`
)

var withDDL = withddl.New([]string{
	fmt.Sprintf(sqlCreateSidecarDB, "_vt"),
	fmt.Sprintf(sqlCreateSchemaMigrationsTable, "_vt"),
})

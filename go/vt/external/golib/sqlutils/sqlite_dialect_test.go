/*
   Copyright 2017 GitHub Inc.

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

/*
	This file has been copied over from VTOrc package
*/

package sqlutils

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var spacesRegexp = regexp.MustCompile(`[\s]+`)

func init() {
}

func stripSpaces(statement string) string {
	statement = strings.TrimSpace(statement)
	statement = spacesRegexp.ReplaceAllString(statement, " ")
	return statement
}

func TestIsCreateTable(t *testing.T) {
	require.True(t, IsCreateTable("create table t(id int)"))
	require.True(t, IsCreateTable(" create table t(id int)"))
	require.True(t, IsCreateTable("CREATE  TABLE t(id int)"))
	require.True(t, IsCreateTable(`
		create table t(id int)
		`))
	require.False(t, IsCreateTable("where create table t(id int)"))
	require.False(t, IsCreateTable("insert"))
}

func TestToSqlite3CreateTable(t *testing.T) {
	{
		statement := "create table t(id int)"
		result := ToSqlite3CreateTable(statement)
		require.Equal(t, result, statement)
	}
	{
		statement := "create table t(id int, v varchar(123) CHARACTER SET ascii NOT NULL default '')"
		result := ToSqlite3CreateTable(statement)
		require.Equal(t, result, "create table t(id int, v varchar(123) NOT NULL default '')")
	}
	{
		statement := "create table t(id int, v varchar ( 123 ) CHARACTER SET ascii NOT NULL default '')"
		result := ToSqlite3CreateTable(statement)
		require.Equal(t, result, "create table t(id int, v varchar ( 123 ) NOT NULL default '')")
	}
	{
		statement := "create table t(i smallint unsigned)"
		result := ToSqlite3CreateTable(statement)
		require.Equal(t, result, "create table t(i smallint)")
	}
	{
		statement := "create table t(i smallint(5) unsigned)"
		result := ToSqlite3CreateTable(statement)
		require.Equal(t, result, "create table t(i smallint)")
	}
	{
		statement := "create table t(i smallint ( 5 ) unsigned)"
		result := ToSqlite3CreateTable(statement)
		require.Equal(t, result, "create table t(i smallint)")
	}
}

func TestToSqlite3AlterTable(t *testing.T) {
	{
		statement := `
			ALTER TABLE
				database_instance
				ADD COLUMN sql_delay INT UNSIGNED NOT NULL AFTER replica_lag_seconds
		`
		result := stripSpaces(ToSqlite3Dialect(statement))
		require.Equal(t, result, stripSpaces(`
			ALTER TABLE
				database_instance
				add column sql_delay int not null default 0
			`))
	}
	{
		statement := `
			ALTER TABLE
				database_instance
				ADD INDEX source_host_port_idx (source_host, source_port)
		`
		result := stripSpaces(ToSqlite3Dialect(statement))
		require.Equal(t, result, stripSpaces(`
			create index
				source_host_port_idx_database_instance
				on database_instance (source_host, source_port)
			`))
	}
	{
		statement := `
				ALTER TABLE
					topology_recovery
					ADD KEY last_detection_idx (last_detection_id)
			`
		result := stripSpaces(ToSqlite3Dialect(statement))
		require.Equal(t, result, stripSpaces(`
			create index
				last_detection_idx_topology_recovery
				on topology_recovery (last_detection_id)
			`))
	}

}

func TestCreateIndex(t *testing.T) {
	{
		statement := `
			create index
				source_host_port_idx_database_instance
				on database_instance (source_host(128), source_port)
		`
		result := stripSpaces(ToSqlite3Dialect(statement))
		require.Equal(t, result, stripSpaces(`
			create index
				source_host_port_idx_database_instance
				on database_instance (source_host, source_port)
			`))
	}
}

func TestIsInsert(t *testing.T) {
	require.True(t, IsInsert("insert into t"))
	require.True(t, IsInsert("insert ignore into t"))
	require.True(t, IsInsert(`
		  insert ignore into t
			`))
	require.False(t, IsInsert("where create table t(id int)"))
	require.False(t, IsInsert("create table t(id int)"))
	require.True(t, IsInsert(`
		insert into
				cluster_domain_name (cluster_name, domain_name, last_registered)
			values
				(?, ?, datetime('now'))
			on duplicate key update
				domain_name=values(domain_name),
				last_registered=values(last_registered)
	`))
}

func TestToSqlite3Insert(t *testing.T) {
	{
		statement := `
			insert into
					cluster_domain_name (cluster_name, domain_name, last_registered)
				values
					(?, ?, datetime('now'))
				on duplicate key update
					domain_name=values(domain_name),
					last_registered=values(last_registered)
		`
		result := stripSpaces(ToSqlite3Dialect(statement))
		require.Equal(t, result, stripSpaces(`
			replace into
					cluster_domain_name (cluster_name, domain_name, last_registered)
				values
					(?, ?, datetime('now'))
			`))
	}
}

func TestToSqlite3GeneralConversions(t *testing.T) {
	{
		statement := "select now()"
		result := ToSqlite3Dialect(statement)
		require.Equal(t, result, "select datetime('now')")
	}
	{
		statement := "select now() - interval ? second"
		result := ToSqlite3Dialect(statement)
		require.Equal(t, result, "select datetime('now', printf('-%d second', ?))")
	}
	{
		statement := "select now() + interval ? minute"
		result := ToSqlite3Dialect(statement)
		require.Equal(t, result, "select datetime('now', printf('+%d minute', ?))")
	}
	{
		statement := "select now() + interval 5 minute"
		result := ToSqlite3Dialect(statement)
		require.Equal(t, result, "select datetime('now', '+5 minute')")
	}
	{
		statement := "select some_table.some_column + interval ? minute"
		result := ToSqlite3Dialect(statement)
		require.Equal(t, result, "select datetime(some_table.some_column, printf('+%d minute', ?))")
	}
	{
		statement := "AND primary_instance.last_attempted_check <= primary_instance.last_seen + interval ? minute"
		result := ToSqlite3Dialect(statement)
		require.Equal(t, result, "AND primary_instance.last_attempted_check <= datetime(primary_instance.last_seen, printf('+%d minute', ?))")
	}
	{
		statement := "select concat(primary_instance.port, '') as port"
		result := ToSqlite3Dialect(statement)
		require.Equal(t, result, "select (primary_instance.port || '') as port")
	}
	{
		statement := "select concat( 'abc' , 'def') as s"
		result := ToSqlite3Dialect(statement)
		require.Equal(t, result, "select ('abc'  || 'def') as s")
	}
	{
		statement := "select concat( 'abc' , 'def', last.col) as s"
		result := ToSqlite3Dialect(statement)
		require.Equal(t, result, "select ('abc'  || 'def' || last.col) as s")
	}
	{
		statement := "select concat(myself.only) as s"
		result := ToSqlite3Dialect(statement)
		require.Equal(t, result, "select concat(myself.only) as s")
	}
	{
		statement := "select concat(1, '2', 3, '4') as s"
		result := ToSqlite3Dialect(statement)
		require.Equal(t, result, "select concat(1, '2', 3, '4') as s")
	}
	{
		statement := "select group_concat( 'abc' , 'def') as s"
		result := ToSqlite3Dialect(statement)
		require.Equal(t, result, "select group_concat( 'abc' , 'def') as s")
	}
}

func TestIsCreateIndex(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"create index my_index on my_table(column);", true},
		{"CREATE INDEX my_index ON my_table(column);", true},
		{"create unique index my_index on my_table(column);", true},
		{"CREATE UNIQUE INDEX my_index ON my_table(column);", true},
		{"create index my_index on my_table(column) where condition;", true},
		{"create unique index my_index on my_table(column) where condition;", true},
		{"create table my_table(column);", false},
		{"drop index my_index on my_table;", false},
		{"alter table my_table add index my_index (column);", false},
		{"", false},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			result := IsCreateIndex(test.input)
			assert.Equal(t, test.expected, result)
		})
	}
}

func TestIsDropIndex(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"drop index my_index on my_table;", true},
		{"DROP INDEX my_index ON my_table;", true},
		{"drop index if exists my_index on my_table;", true},
		{"DROP INDEX IF EXISTS my_index ON my_table;", true},
		{"drop table my_table;", false},
		{"create index my_index on my_table(column);", false},
		{"alter table my_table add index my_index (column);", false},
		{"", false},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			result := IsDropIndex(test.input)
			assert.Equal(t, test.expected, result)
		})
	}
}

func TestToSqlite3Dialect(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"create table my_table(id int);", "create table my_table(id int);"},
		{"alter table my_table add column new_col int;", "alter table my_table add column new_col int;"},
		{"insert into my_table values (1);", "insert into my_table values (1);"},
		{"", ""},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			result := ToSqlite3Dialect(test.input)
			assert.Equal(t, test.expected, result)
		})
	}
}

func buildToSqlite3Dialect_Insert(instances int) string {
	var rows []string
	for i := 0; i < instances; i++ {
		rows = append(rows, `(?, ?, ?, NOW(), NOW(), 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,     ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())`)
	}

	return fmt.Sprintf(`INSERT ignore INTO database_instance
			(alias, hostname, port, last_checked, last_attempted_check, last_check_partial_success, server_id, server_uuid,
			version, major_version, version_comment, binlog_server, read_only, binlog_format,
			binlog_row_image, log_bin, log_replica_updates, binary_log_file, binary_log_pos, source_host, source_port, replica_net_timeout, heartbeat_interval,
			replica_sql_running, replica_io_running, replication_sql_thread_state, replication_io_thread_state, has_replication_filters, supports_oracle_gtid, oracle_gtid, source_uuid, ancestry_uuid, executed_gtid_set, gtid_mode, gtid_purged, gtid_errant, mariadb_gtid, pseudo_gtid,
			source_log_file, read_source_log_pos, relay_source_log_file, exec_source_log_pos, relay_log_file, relay_log_pos, last_sql_error, last_io_error, replication_lag_seconds, replica_lag_seconds, sql_delay, data_center, region, physical_environment, replication_depth, is_co_primary, has_replication_credentials, allow_tls, semi_sync_enforced, semi_sync_primary_enabled, semi_sync_primary_timeout, semi_sync_primary_wait_for_replica_count, semi_sync_replica_enabled, semi_sync_primary_status, semi_sync_primary_clients, semi_sync_replica_status, last_discovery_latency, last_seen)
		VALUES
			%s
		ON DUPLICATE KEY UPDATE
			alias=VALUES(alias), hostname=VALUES(hostname), port=VALUES(port), last_checked=VALUES(last_checked), last_attempted_check=VALUES(last_attempted_check), last_check_partial_success=VALUES(last_check_partial_success), server_id=VALUES(server_id), server_uuid=VALUES(server_uuid), version=VALUES(version), major_version=VALUES(major_version), version_comment=VALUES(version_comment), binlog_server=VALUES(binlog_server), read_only=VALUES(read_only), binlog_format=VALUES(binlog_format), binlog_row_image=VALUES(binlog_row_image), log_bin=VALUES(log_bin), log_replica_updates=VALUES(log_replica_updates), binary_log_file=VALUES(binary_log_file), binary_log_pos=VALUES(binary_log_pos), source_host=VALUES(source_host), source_port=VALUES(source_port), replica_net_timeout=VALUES(replica_net_timeout), heartbeat_interval=VALUES(heartbeat_interval), replica_sql_running=VALUES(replica_sql_running), replica_io_running=VALUES(replica_io_running), replication_sql_thread_state=VALUES(replication_sql_thread_state), replication_io_thread_state=VALUES(replication_io_thread_state), has_replication_filters=VALUES(has_replication_filters), supports_oracle_gtid=VALUES(supports_oracle_gtid), oracle_gtid=VALUES(oracle_gtid), source_uuid=VALUES(source_uuid), ancestry_uuid=VALUES(ancestry_uuid), executed_gtid_set=VALUES(executed_gtid_set), gtid_mode=VALUES(gtid_mode), gtid_purged=VALUES(gtid_purged), gtid_errant=VALUES(gtid_errant), mariadb_gtid=VALUES(mariadb_gtid), pseudo_gtid=VALUES(pseudo_gtid), source_log_file=VALUES(source_log_file), read_source_log_pos=VALUES(read_source_log_pos), relay_source_log_file=VALUES(relay_source_log_file), exec_source_log_pos=VALUES(exec_source_log_pos), relay_log_file=VALUES(relay_log_file), relay_log_pos=VALUES(relay_log_pos), last_sql_error=VALUES(last_sql_error), last_io_error=VALUES(last_io_error), replication_lag_seconds=VALUES(replication_lag_seconds), replica_lag_seconds=VALUES(replica_lag_seconds), sql_delay=VALUES(sql_delay), data_center=VALUES(data_center), region=VALUES(region), physical_environment=VALUES(physical_environment), replication_depth=VALUES(replication_depth), is_co_primary=VALUES(is_co_primary), has_replication_credentials=VALUES(has_replication_credentials), allow_tls=VALUES(allow_tls),
			semi_sync_enforced=VALUES(semi_sync_enforced), semi_sync_primary_enabled=VALUES(semi_sync_primary_enabled), semi_sync_primary_timeout=VALUES(semi_sync_primary_timeout), semi_sync_primary_wait_for_replica_count=VALUES(semi_sync_primary_wait_for_replica_count), semi_sync_replica_enabled=VALUES(semi_sync_replica_enabled), semi_sync_primary_status=VALUES(semi_sync_primary_status), semi_sync_primary_clients=VALUES(semi_sync_primary_clients), semi_sync_replica_status=VALUES(semi_sync_replica_status),
			last_discovery_latency=VALUES(last_discovery_latency), last_seen=VALUES(last_seen)
       `, strings.Join(rows, "\n\t\t\t\t"))
}

func BenchmarkToSqlite3Dialect_Insert1000(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		statement := buildToSqlite3Dialect_Insert(1000)
		b.StartTimer()
		ToSqlite3Dialect(statement)
	}
}

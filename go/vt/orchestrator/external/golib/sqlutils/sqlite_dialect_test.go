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

package sqlutils

import (
	"regexp"
	"strings"
	"testing"

	test "vitess.io/vitess/go/vt/orchestrator/external/golib/tests"
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
	test.S(t).ExpectTrue(IsCreateTable("create table t(id int)"))
	test.S(t).ExpectTrue(IsCreateTable(" create table t(id int)"))
	test.S(t).ExpectTrue(IsCreateTable("CREATE  TABLE t(id int)"))
	test.S(t).ExpectTrue(IsCreateTable(`
		create table t(id int)
		`))
	test.S(t).ExpectFalse(IsCreateTable("where create table t(id int)"))
	test.S(t).ExpectFalse(IsCreateTable("insert"))
}

func TestToSqlite3CreateTable(t *testing.T) {
	{
		statement := "create table t(id int)"
		result := ToSqlite3CreateTable(statement)
		test.S(t).ExpectEquals(result, statement)
	}
	{
		statement := "create table t(id int, v varchar(123) CHARACTER SET ascii NOT NULL default '')"
		result := ToSqlite3CreateTable(statement)
		test.S(t).ExpectEquals(result, "create table t(id int, v varchar(123) NOT NULL default '')")
	}
	{
		statement := "create table t(id int, v varchar ( 123 ) CHARACTER SET ascii NOT NULL default '')"
		result := ToSqlite3CreateTable(statement)
		test.S(t).ExpectEquals(result, "create table t(id int, v varchar ( 123 ) NOT NULL default '')")
	}
	{
		statement := "create table t(i smallint unsigned)"
		result := ToSqlite3CreateTable(statement)
		test.S(t).ExpectEquals(result, "create table t(i smallint)")
	}
	{
		statement := "create table t(i smallint(5) unsigned)"
		result := ToSqlite3CreateTable(statement)
		test.S(t).ExpectEquals(result, "create table t(i smallint)")
	}
	{
		statement := "create table t(i smallint ( 5 ) unsigned)"
		result := ToSqlite3CreateTable(statement)
		test.S(t).ExpectEquals(result, "create table t(i smallint)")
	}
}

func TestToSqlite3AlterTable(t *testing.T) {
	{
		statement := `
			ALTER TABLE
				database_instance
				ADD COLUMN sql_delay INT UNSIGNED NOT NULL AFTER slave_lag_seconds
		`
		result := stripSpaces(ToSqlite3Dialect(statement))
		test.S(t).ExpectEquals(result, stripSpaces(`
			ALTER TABLE
				database_instance
				add column sql_delay int not null default 0
			`))
	}
	{
		statement := `
			ALTER TABLE
				database_instance
				ADD INDEX master_host_port_idx (master_host, master_port)
		`
		result := stripSpaces(ToSqlite3Dialect(statement))
		test.S(t).ExpectEquals(result, stripSpaces(`
			create index
				master_host_port_idx_database_instance
				on database_instance (master_host, master_port)
			`))
	}
	{
		statement := `
				ALTER TABLE
					topology_recovery
					ADD KEY last_detection_idx (last_detection_id)
			`
		result := stripSpaces(ToSqlite3Dialect(statement))
		test.S(t).ExpectEquals(result, stripSpaces(`
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
				master_host_port_idx_database_instance
				on database_instance (master_host(128), master_port)
		`
		result := stripSpaces(ToSqlite3Dialect(statement))
		test.S(t).ExpectEquals(result, stripSpaces(`
			create index
				master_host_port_idx_database_instance
				on database_instance (master_host, master_port)
			`))
	}
}

func TestIsInsert(t *testing.T) {
	test.S(t).ExpectTrue(IsInsert("insert into t"))
	test.S(t).ExpectTrue(IsInsert("insert ignore into t"))
	test.S(t).ExpectTrue(IsInsert(`
		  insert ignore into t
			`))
	test.S(t).ExpectFalse(IsInsert("where create table t(id int)"))
	test.S(t).ExpectFalse(IsInsert("create table t(id int)"))
	test.S(t).ExpectTrue(IsInsert(`
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
		test.S(t).ExpectEquals(result, stripSpaces(`
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
		test.S(t).ExpectEquals(result, "select datetime('now')")
	}
	{
		statement := "select now() - interval ? second"
		result := ToSqlite3Dialect(statement)
		test.S(t).ExpectEquals(result, "select datetime('now', printf('-%d second', ?))")
	}
	{
		statement := "select now() + interval ? minute"
		result := ToSqlite3Dialect(statement)
		test.S(t).ExpectEquals(result, "select datetime('now', printf('+%d minute', ?))")
	}
	{
		statement := "select now() + interval 5 minute"
		result := ToSqlite3Dialect(statement)
		test.S(t).ExpectEquals(result, "select datetime('now', '+5 minute')")
	}
	{
		statement := "select some_table.some_column + interval ? minute"
		result := ToSqlite3Dialect(statement)
		test.S(t).ExpectEquals(result, "select datetime(some_table.some_column, printf('+%d minute', ?))")
	}
	{
		statement := "AND master_instance.last_attempted_check <= master_instance.last_seen + interval ? minute"
		result := ToSqlite3Dialect(statement)
		test.S(t).ExpectEquals(result, "AND master_instance.last_attempted_check <= datetime(master_instance.last_seen, printf('+%d minute', ?))")
	}
	{
		statement := "select concat(master_instance.port, '') as port"
		result := ToSqlite3Dialect(statement)
		test.S(t).ExpectEquals(result, "select (master_instance.port || '') as port")
	}
	{
		statement := "select concat( 'abc' , 'def') as s"
		result := ToSqlite3Dialect(statement)
		test.S(t).ExpectEquals(result, "select ('abc'  || 'def') as s")
	}
	{
		statement := "select concat( 'abc' , 'def', last.col) as s"
		result := ToSqlite3Dialect(statement)
		test.S(t).ExpectEquals(result, "select ('abc'  || 'def' || last.col) as s")
	}
	{
		statement := "select concat(myself.only) as s"
		result := ToSqlite3Dialect(statement)
		test.S(t).ExpectEquals(result, "select concat(myself.only) as s")
	}
	{
		statement := "select concat(1, '2', 3, '4') as s"
		result := ToSqlite3Dialect(statement)
		test.S(t).ExpectEquals(result, "select concat(1, '2', 3, '4') as s")
	}
	{
		statement := "select group_concat( 'abc' , 'def') as s"
		result := ToSqlite3Dialect(statement)
		test.S(t).ExpectEquals(result, "select group_concat( 'abc' , 'def') as s")
	}
}

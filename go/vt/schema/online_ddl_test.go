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

package schema

import (
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/sqlparser"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vttime"
)

func TestCreateUUID(t *testing.T) {
	_, err := CreateUUIDWithDelimiter("_")
	assert.NoError(t, err)
}

func TestIsOnlineDDLUUID(t *testing.T) {
	for i := 0; i < 20; i++ {
		uuid, err := CreateOnlineDDLUUID()
		assert.NoError(t, err)
		assert.True(t, IsOnlineDDLUUID(uuid))
	}
	tt := []string{
		"a0638f6b_ec7b_11ea_9bf8_000d3a9b8a9a_", // suffix invalid
		"_a0638f6b_ec7b_11ea_9bf8_000d3a9b8a9a", // prefix invalid
		"a0638f6b_ec7b_11ea_9bf8_000d3a9b8a9z",  // "z" character invalid
		"a0638f6b-ec7b-11ea-9bf8-000d3a9b8a9a",  // dash invalid
		"a0638f6b_ec7b_11ea_9bf8_000d3a9b8a9",   // too short
	}
	for _, tc := range tt {
		assert.False(t, IsOnlineDDLUUID(tc))
	}
}

func TestGetGCUUID(t *testing.T) {
	uuids := map[string]bool{}
	count := 20
	for i := 0; i < count; i++ {
		onlineDDL, err := NewOnlineDDL("ks", "tbl", "alter table t drop column c", NewDDLStrategySetting(tabletmanagerdatapb.OnlineDDL_DIRECT, ""), "", "")
		assert.NoError(t, err)
		gcUUID := onlineDDL.GetGCUUID()
		assert.True(t, IsGCUUID(gcUUID))
		uuids[gcUUID] = true
	}
	assert.Equal(t, count, len(uuids))
}
func TestGetActionStr(t *testing.T) {
	tt := []struct {
		statement string
		actionStr string
		isError   bool
	}{
		{
			statement: "create table t (id int primary key)",
			actionStr: sqlparser.CreateStr,
		},
		{
			statement: "alter table t drop column c",
			actionStr: sqlparser.AlterStr,
		},
		{
			statement: "drop table t",
			actionStr: sqlparser.DropStr,
		},
		{
			statement: "rename table t to t2",
			isError:   true,
		},
	}
	for _, ts := range tt {
		t.Run(ts.statement, func(t *testing.T) {
			onlineDDL := &OnlineDDL{
				OnlineDDL: &tabletmanagerdatapb.OnlineDDL{
					Sql: ts.statement,
				},
			}
			_, actionStr, err := onlineDDL.GetActionStr()
			if ts.isError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, actionStr, ts.actionStr)
			}
		})
	}
}

func TestIsOnlineDDLTableName(t *testing.T) {
	names := []string{
		"_4e5dcf80_354b_11eb_82cd_f875a4d24e90_20201203114014_gho",
		"_4e5dcf80_354b_11eb_82cd_f875a4d24e90_20201203114014_ghc",
		"_4e5dcf80_354b_11eb_82cd_f875a4d24e90_20201203114014_del",
		"_4e5dcf80_354b_11eb_82cd_f875a4d24e90_20201203114013_new",
		"_84371a37_6153_11eb_9917_f875a4d24e90_20210128122816_vrepl",
		"_table_old",
		"__table_old",
	}
	for _, tableName := range names {
		assert.True(t, IsOnlineDDLTableName(tableName))
	}
	irrelevantNames := []string{
		"t",
		"_table_new",
		"__table_new",
		"_table_gho",
		"_table_ghc",
		"_table_del",
		"_table_vrepl",
		"table_old",
	}
	for _, tableName := range irrelevantNames {
		assert.False(t, IsOnlineDDLTableName(tableName))
	}
}

func TestGetRevertUUID(t *testing.T) {
	tt := []struct {
		statement string
		uuid      string
		isError   bool
	}{
		{
			statement: "revert 4e5dcf80_354b_11eb_82cd_f875a4d24e90",
			uuid:      "4e5dcf80_354b_11eb_82cd_f875a4d24e90",
		},
		{
			statement: "REVERT   4e5dcf80_354b_11eb_82cd_f875a4d24e90",
			uuid:      "4e5dcf80_354b_11eb_82cd_f875a4d24e90",
		},
		{
			statement: "alter table t drop column c",
			isError:   true,
		},
	}
	for _, ts := range tt {
		t.Run(ts.statement, func(t *testing.T) {
			onlineDDL := &OnlineDDL{
				OnlineDDL: &tabletmanagerdatapb.OnlineDDL{
					Sql: ts.statement,
				},
			}
			uuid, err := onlineDDL.GetRevertUUID()
			if ts.isError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, ts.uuid, uuid)
		})
	}
	migrationContext := "354b-11eb-82cd-f875a4d24e90"
	for _, ts := range tt {
		t.Run(ts.statement, func(t *testing.T) {
			onlineDDL, err := NewOnlineDDL("test_ks", "t", ts.statement, NewDDLStrategySetting(tabletmanagerdatapb.OnlineDDL_ONLINE, ""), migrationContext, "")
			assert.NoError(t, err)
			require.NotNil(t, onlineDDL)
			uuid, err := onlineDDL.GetRevertUUID()
			if ts.isError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, ts.uuid, uuid)
		})
	}
}

func TestNewOnlineDDL(t *testing.T) {
	migrationContext := "354b-11eb-82cd-f875a4d24e90"
	tt := []struct {
		sql     string
		isError bool
	}{
		{
			sql: "drop table t",
		},
		{
			sql: "create table t (id int primary key)",
		},
		{
			sql: "alter table t engine=innodb",
		},
		{
			sql: "revert vitess_migration '4e5dcf80_354b_11eb_82cd_f875a4d24e90'",
		},
		{
			sql:     "alter vitess_migration '4e5dcf80_354b_11eb_82cd_f875a4d24e90' cancel",
			isError: true,
		},
		{
			sql:     "select id from t",
			isError: true,
		},
	}
	strategies := []*DDLStrategySetting{
		NewDDLStrategySetting(tabletmanagerdatapb.OnlineDDL_DIRECT, ""),
		NewDDLStrategySetting(tabletmanagerdatapb.OnlineDDL_VITESS, ""),
		NewDDLStrategySetting(tabletmanagerdatapb.OnlineDDL_ONLINE, "-singleton"),
	}

	for _, ts := range tt {
		t.Run(ts.sql, func(t *testing.T) {
			for _, stgy := range strategies {
				t.Run(stgy.ToString(), func(t *testing.T) {
					onlineDDL, err := NewOnlineDDL("test_ks", "t", ts.sql, stgy, migrationContext, "")
					if ts.isError {
						assert.Error(t, err)
						return
					}
					assert.NoError(t, err)
					// onlineDDL.SQL enriched with /*vt+ ... */ comment
					assert.Contains(t, onlineDDL.Sql, hex.EncodeToString([]byte(onlineDDL.Uuid)))
					assert.Contains(t, onlineDDL.Sql, hex.EncodeToString([]byte(migrationContext)))
					assert.Contains(t, onlineDDL.Sql, hex.EncodeToString([]byte(OnlineDDLStrategyName(stgy.Strategy))))
				})
			}
		})
	}

	t.Run("explicit UUID", func(t *testing.T) {
		var err error
		var onlineDDL *OnlineDDL

		onlineDDL, err = NewOnlineDDL("test_ks", "t", "alter table t engine=innodb", NewDDLStrategySetting(tabletmanagerdatapb.OnlineDDL_VITESS, ""), migrationContext, "")
		assert.NoError(t, err)
		assert.True(t, IsOnlineDDLUUID(onlineDDL.Uuid))

		_, err = NewOnlineDDL("test_ks", "t", "alter table t engine=innodb", NewDDLStrategySetting(tabletmanagerdatapb.OnlineDDL_ONLINE, ""), migrationContext, "abc")
		assert.Error(t, err)

		onlineDDL, err = NewOnlineDDL("test_ks", "t", "alter table t engine=innodb", NewDDLStrategySetting(tabletmanagerdatapb.OnlineDDL_VITESS, ""), migrationContext, "4e5dcf80_354b_11eb_82cd_f875a4d24e90")
		assert.NoError(t, err)
		assert.Equal(t, "4e5dcf80_354b_11eb_82cd_f875a4d24e90", onlineDDL.Uuid)

		_, err = NewOnlineDDL("test_ks", "t", "alter table t engine=innodb", NewDDLStrategySetting(tabletmanagerdatapb.OnlineDDL_VITESS, ""), migrationContext, " 4e5dcf80_354b_11eb_82cd_f875a4d24e90")
		assert.Error(t, err)
	})
}

func TestNewOnlineDDLs(t *testing.T) {
	type expect struct {
		sqls            []string
		notDDL          bool
		parseError      bool
		isError         bool
		expectErrorText string
		isView          bool
	}
	tests := map[string]expect{
		"alter table t add column i int, drop column d": {sqls: []string{"alter table t add column i int, drop column d"}},
		"create table t (id int primary key)":           {sqls: []string{"create table t (id int primary key)"}},
		"drop table t":                                  {sqls: []string{"drop table t"}},
		"drop table if exists t":                        {sqls: []string{"drop table if exists t"}},
		"drop table t1, t2, t3":                         {sqls: []string{"drop table t1", "drop table t2", "drop table t3"}},
		"drop table if exists t1, t2, t3":               {sqls: []string{"drop table if exists t1", "drop table if exists t2", "drop table if exists t3"}},
		"create index i_idx on t(id)":                   {sqls: []string{"alter table t add index i_idx (id)"}},
		"create index i_idx on t(name(12))":             {sqls: []string{"alter table t add index i_idx (`name`(12))"}},
		"create index i_idx on t(id, `ts`, name(12))":   {sqls: []string{"alter table t add index i_idx (id, ts, `name`(12))"}},
		"create unique index i_idx on t(id)":            {sqls: []string{"alter table t add unique index i_idx (id)"}},
		"create index i_idx using btree on t(id)":       {sqls: []string{"alter table t add index i_idx (id) using btree"}},
		"create view v as select * from t":              {sqls: []string{"create view v as select * from t"}, isView: true},
		"alter view v as select * from t":               {sqls: []string{"alter view v as select * from t"}, isView: true},
		"drop view v":                                   {sqls: []string{"drop view v"}, isView: true},
		"drop view if exists v":                         {sqls: []string{"drop view if exists v"}, isView: true},
		"create index with syntax error i_idx on t(id)": {parseError: true},
		"select * from t":                               {notDDL: true},
		"drop database t":                               {notDDL: true},
		"truncate table t":                              {isError: true},
		"rename table t to t1":                          {isError: true},
		"alter table corder add FOREIGN KEY my_fk(customer_id) reference customer(customer_id)":                                                                                      {isError: true, expectErrorText: "syntax error"},
		"alter table corder add FOREIGN KEY my_fk(customer_id) references customer(customer_id)":                                                                                     {isError: true, expectErrorText: "foreign key constraints are not supported"},
		"alter table corder rename as something_else":                                                                                                                                {isError: true, expectErrorText: "RENAME is not supported in online DDL"},
		"CREATE TABLE if not exists t (id bigint unsigned NOT NULL AUTO_INCREMENT, ts datetime(6) DEFAULT NULL, error_column NO_SUCH_TYPE NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB": {isError: true, expectErrorText: "near"},
	}
	migrationContext := "354b-11eb-82cd-f875a4d24e90"
	for query, expect := range tests {
		t.Run(query, func(t *testing.T) {
			stmt, err := sqlparser.Parse(query)
			if expect.parseError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			ddlStmt, ok := stmt.(sqlparser.DDLStatement)
			if expect.notDDL {
				assert.False(t, ok)
				return
			}
			assert.True(t, ok)

			onlineDDLs, err := NewOnlineDDLs("test_ks", query, ddlStmt, NewDDLStrategySetting(tabletmanagerdatapb.OnlineDDL_VITESS, ""), migrationContext, "")
			if expect.isError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), expect.expectErrorText)
				return
			}
			assert.NoError(t, err)

			sqls := []string{}
			for _, onlineDDL := range onlineDDLs {
				sql, err := onlineDDL.sqlWithoutComments()
				assert.NoError(t, err)
				sql = strings.ReplaceAll(sql, "\n", "")
				sql = strings.ReplaceAll(sql, "\t", "")
				sqls = append(sqls, sql)
				assert.Equal(t, expect.isView, onlineDDL.IsView())
			}
			assert.Equal(t, expect.sqls, sqls)
		})
	}
}

func TestNewOnlineDDLsForeignKeys(t *testing.T) {
	type expect struct {
		sqls            []string
		notDDL          bool
		parseError      bool
		isError         bool
		expectErrorText string
		isView          bool
	}
	queries := []string{
		"alter table corder add FOREIGN KEY my_fk(customer_id) references customer(customer_id)",
		"create table t1 (id int primary key, i int, foreign key (i) references parent(id))",
	}

	migrationContext := "354b-11eb-82cd-f875a4d24e90"
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			for _, allowForeignKeys := range []bool{false, true} {
				testName := fmt.Sprintf("%t", allowForeignKeys)
				t.Run(testName, func(t *testing.T) {
					stmt, err := sqlparser.Parse(query)
					require.NoError(t, err)
					ddlStmt, ok := stmt.(sqlparser.DDLStatement)
					require.True(t, ok)

					flags := ""
					if allowForeignKeys {
						flags = "--unsafe-allow-foreign-keys"
					}
					onlineDDLs, err := NewOnlineDDLs("test_ks", query, ddlStmt, NewDDLStrategySetting(tabletmanagerdatapb.OnlineDDL_VITESS, flags), migrationContext, "")
					if allowForeignKeys {
						assert.NoError(t, err)
					} else {
						assert.Error(t, err)
						assert.Contains(t, err.Error(), "foreign key constraints are not supported")
					}

					for _, onlineDDL := range onlineDDLs {
						sql, err := onlineDDL.sqlWithoutComments()
						assert.NoError(t, err)
						assert.NotEmpty(t, sql)
					}
				})
			}
		})
	}
}

func TestOnlineDDLFromCommentedStatement(t *testing.T) {
	queries := []string{
		`create table t (id int primary key)`,
		`alter table t drop primary key`,
		`drop table if exists t`,
		`create view v as select * from t`,
		`drop view v`,
		`alter view v as select * from t`,
		`revert vitess_migration '4e5dcf80_354b_11eb_82cd_f875a4d24e90'`,
	}
	strategySetting := NewDDLStrategySetting(tabletmanagerdatapb.OnlineDDL_GHOST, `-singleton -declarative --max-load="Threads_running=5"`)
	migrationContext := "354b-11eb-82cd-f875a4d24e90"
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			o1, err := NewOnlineDDL("ks", "t", query, strategySetting, migrationContext, "")
			require.NoError(t, err)

			stmt, err := sqlparser.Parse(o1.Sql)
			require.NoError(t, err)

			o2, err := OnlineDDLFromCommentedStatement(stmt)
			require.NoError(t, err)
			assert.True(t, IsOnlineDDLUUID(o2.Uuid))
			assert.Equal(t, o1.Uuid, o2.Uuid)
			assert.Equal(t, migrationContext, o2.MigrationContext)
			assert.Equal(t, "t", o2.Table)
			assert.Equal(t, strategySetting.Strategy, o2.Strategy)
			assert.Equal(t, strategySetting.Options, o2.Options)
		})
	}
}

func TestFromJSON(t *testing.T) {
	uuid, err := CreateUUIDWithDelimiter("_")
	require.NoError(t, err, "failed to create UUID for tests")

	type testcase struct {
		json      string
		expected  *OnlineDDL
		shouldErr bool
	}
	tests := []testcase{
		{
			json:     "{}",
			expected: &OnlineDDL{OnlineDDL: &tabletmanagerdatapb.OnlineDDL{}},
		},
		{
			// (andrew) TODO: expand this test case to contain all the struct
			// fields.
			json: fmt.Sprintf(`{
				"keyspace": "ks",
				"table": "t1",
				"schema": "CREATE TABLE t1 (id int(11) NOT NULL PRIMARY KEY)",
				"sql": "ALTER TABLE t1 ADD COLUMN i INT",
				"uuid": "%s",
				"strategy": "VITESS",
				"options": "--allow-concurrent",
				"retries": 2
			}`, uuid),
			expected: &OnlineDDL{
				OnlineDDL: &tabletmanagerdatapb.OnlineDDL{
					Keyspace: "ks",
					Table:    "t1",
					Schema:   "CREATE TABLE t1 (id int(11) NOT NULL PRIMARY KEY)",
					Sql:      "ALTER TABLE t1 ADD COLUMN i INT",
					Uuid:     uuid,
					Strategy: tabletmanagerdatapb.OnlineDDL_VITESS,
					Options:  "--allow-concurrent",
					Retries:  2,
				},
			},
		},
	}

	for _, test := range tests {
		test := test

		t.Run("", func(t *testing.T) {
			actual, err := FromJSON([]byte(test.json))
			if test.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			utils.MustMatch(t, test.expected, actual)
		})
	}

	t.Run("strategy parsing", func(t *testing.T) {
		tests := []testcase{
			{
				json: `{"strategy": "pt-osc"}`,
				expected: &OnlineDDL{
					OnlineDDL: &tabletmanagerdatapb.OnlineDDL{
						Strategy: tabletmanagerdatapb.OnlineDDL_PTOSC,
					},
				},
			},
			{
				json: `{"strategy": "GHOST"}`,
				expected: &OnlineDDL{
					OnlineDDL: &tabletmanagerdatapb.OnlineDDL{
						Strategy: tabletmanagerdatapb.OnlineDDL_GHOST,
					},
				},
			},
			{
				json: `{"strategy": 3}`,
				expected: &OnlineDDL{
					OnlineDDL: &tabletmanagerdatapb.OnlineDDL{
						Strategy: tabletmanagerdatapb.OnlineDDL_DIRECT,
					},
				},
			},
			{
				json:      `{"strategy": "not a strategy"}`,
				shouldErr: true,
			},
		}

		for _, test := range tests {
			test := test

			t.Run("", func(t *testing.T) {
				actual, err := FromJSON([]byte(test.json))
				if test.shouldErr {
					assert.Error(t, err)
					return
				}

				require.NoError(t, err)
				utils.MustMatch(t, test.expected, actual)
			})
		}
	})

	t.Run("tablet alias parsing", func(t *testing.T) {
		tests := []testcase{
			{
				json: `{"tablet": "zone1-101"}`,
				expected: &OnlineDDL{
					TabletAlias: "zone1-101",
					OnlineDDL: &tabletmanagerdatapb.OnlineDDL{
						TabletAlias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
					},
				},
			},
			{
				json: `{
					"tablet_alias": {
						"cell": "zone1",
						"uid": 101
					}
				}`,
				expected: &OnlineDDL{
					TabletAlias: "zone1-101",
					OnlineDDL: &tabletmanagerdatapb.OnlineDDL{
						TabletAlias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
					},
				},
			},
			{
				json: `{
					"context": "abcd",
					"tablet": "zone1-404",
					"tablet_alias": {
						"cell": "zone1",
						"uid": 101
					}
				}`,
				expected: &OnlineDDL{
					TabletAlias: "zone1-101",
					OnlineDDL: &tabletmanagerdatapb.OnlineDDL{
						TabletAlias: &topodatapb.TabletAlias{
							Cell: "zone1",
							Uid:  101,
						},
					},
				},
			},
		}

		for _, test := range tests {
			test := test

			t.Run("", func(t *testing.T) {
				actual, err := FromJSON([]byte(test.json))
				if test.shouldErr {
					assert.Error(t, err)
					return
				}

				require.NoError(t, err)
				utils.MustMatch(t, test.expected, actual)
			})
		}
	})

	t.Run("request_time parsing", func(t *testing.T) {
		tests := []testcase{
			{
				json: `{
					"request_time": {
						"seconds": 10,
						"nanoseconds": 5
					}
				}`,
				expected: &OnlineDDL{
					RequestTime: 10e9 + 5,
					OnlineDDL: &tabletmanagerdatapb.OnlineDDL{
						RequestTime: &vttime.Time{
							Seconds:     10,
							Nanoseconds: 5,
						},
					},
				},
			},
			{
				json: fmt.Sprintf(
					`{"time_created": %f}`,
					3*1e9+2, // 3 seconds and 2 nanos
				),
				expected: &OnlineDDL{
					RequestTime: 3e9 + 2,
					OnlineDDL: &tabletmanagerdatapb.OnlineDDL{
						RequestTime: &vttime.Time{
							Seconds:     3,
							Nanoseconds: 2,
						},
					},
				},
			},
			{
				json: fmt.Sprintf(
					`{
						"time_created": %f,
						"request_time": {
							"seconds": 10,
							"nanoseconds": 5
						}
					}`,
					3*1e9+2, // 3 seconds and 2 nanos
				),
				expected: &OnlineDDL{
					RequestTime: 10e9 + 5,
					OnlineDDL: &tabletmanagerdatapb.OnlineDDL{
						RequestTime: &vttime.Time{
							Seconds:     10,
							Nanoseconds: 5,
						},
					},
				},
			},
		}

		for _, test := range tests {
			test := test

			t.Run("", func(t *testing.T) {
				actual, err := FromJSON([]byte(test.json))
				if test.shouldErr {
					assert.Error(t, err)
					return
				}

				require.NoError(t, err)
				utils.MustMatch(t, test.expected, actual)
			})
		}
	})
}

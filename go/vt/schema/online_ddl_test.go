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
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestCreateUUID(t *testing.T) {
	_, err := createUUID("_")
	assert.NoError(t, err)
}

func TestIsDirect(t *testing.T) {
	assert.True(t, DDLStrategyDirect.IsDirect())
	assert.False(t, DDLStrategyOnline.IsDirect())
	assert.False(t, DDLStrategyGhost.IsDirect())
	assert.False(t, DDLStrategyPTOSC.IsDirect())
	assert.True(t, DDLStrategy("").IsDirect())
	assert.False(t, DDLStrategy("gh-ost").IsDirect())
	assert.False(t, DDLStrategy("pt-osc").IsDirect())
	assert.True(t, DDLStrategy("something").IsDirect())
}

func TestParseDDLStrategy(t *testing.T) {
	tt := []struct {
		strategyVariable string
		strategy         DDLStrategy
		options          string
		err              error
	}{
		{
			strategyVariable: "direct",
			strategy:         DDLStrategyDirect,
		},
		{
			strategyVariable: "online",
			strategy:         DDLStrategyOnline,
		},
		{
			strategyVariable: "gh-ost",
			strategy:         DDLStrategyGhost,
		},
		{
			strategyVariable: "pt-osc",
			strategy:         DDLStrategyPTOSC,
		},
		{
			strategy: DDLStrategyDirect,
		},
		{
			strategyVariable: "gh-ost --max-load=Threads_running=100 --allow-master",
			strategy:         DDLStrategyGhost,
			options:          "--max-load=Threads_running=100 --allow-master",
		},
	}
	for _, ts := range tt {
		strategy, options, err := ParseDDLStrategy(ts.strategyVariable)
		assert.NoError(t, err)
		assert.Equal(t, ts.strategy, strategy)
		assert.Equal(t, ts.options, options)
	}
	{
		_, _, err := ParseDDLStrategy("other")
		assert.Error(t, err)
	}
}

func TestIsOnlineDDLUUID(t *testing.T) {
	for i := 0; i < 20; i++ {
		uuid, err := createUUID("_")
		assert.NoError(t, err)
		assert.True(t, IsOnlineDDLUUID(uuid))
	}
	tt := []string{
		"a0638f6b_ec7b_11ea_9bf8_000d3a9b8a9a_",
		"_a0638f6b_ec7b_11ea_9bf8_000d3a9b8a9a",
		"a0638f6b_ec7b_11ea_9bf8_000d3a9b8a9z",
		"a0638f6b-ec7b-11ea-9bf8-000d3a9b8a9a",
		"a0638f6b_ec7b_11ea_9bf8_000d3a9b8a9",
	}
	for _, tc := range tt {
		assert.False(t, IsOnlineDDLUUID(tc))
	}
}

func TestGetGCUUID(t *testing.T) {
	uuids := map[string]bool{}
	count := 20
	for i := 0; i < count; i++ {
		onlineDDL, err := NewOnlineDDL("ks", "tbl", "alter table t drop column c", DDLStrategyDirect, "", "")
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
			onlineDDL := &OnlineDDL{SQL: ts.statement}
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
			statement: "revert 4e5dcf80_354b_11eb_82cd_f875a4d24e90_20201203114014",
			uuid:      "4e5dcf80_354b_11eb_82cd_f875a4d24e90_20201203114014",
		},
		{
			statement: "REVERT   4e5dcf80_354b_11eb_82cd_f875a4d24e90_20201203114014",
			uuid:      "4e5dcf80_354b_11eb_82cd_f875a4d24e90_20201203114014",
		},
		{
			statement: "REVERT",
			isError:   true,
		},
		{
			statement: "alter table t drop column c",
			isError:   true,
		},
	}
	for _, ts := range tt {
		t.Run(ts.statement, func(t *testing.T) {
			onlineDDL := &OnlineDDL{SQL: ts.statement}
			uuid, err := onlineDDL.GetRevertUUID()
			if ts.isError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, uuid, ts.uuid)
			}
		})
	}
}

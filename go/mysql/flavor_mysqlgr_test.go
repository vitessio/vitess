/*
Copyright 2021 The Vitess Authors.

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
package mysql

import (
	"testing"

	"gotest.tools/assert"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestMysqlGRParsePrimaryGroupMember(t *testing.T) {
	res := ReplicationStatus{}
	rows := []sqltypes.Value{
		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("host1")),
		sqltypes.MakeTrusted(querypb.Type_INT32, []byte("10")),
	}
	parsePrimaryGroupMember(&res, rows)
	assert.Equal(t, "host1", res.SourceHost)
	assert.Equal(t, 10, res.SourcePort)
	assert.Equal(t, ReplicationStateUnknown, res.IOState)
	assert.Equal(t, ReplicationStateUnknown, res.SQLState)
}

func TestMysqlGRReplicationApplierLagParse(t *testing.T) {
	res := ReplicationStatus{}
	row := []sqltypes.Value{
		sqltypes.MakeTrusted(querypb.Type_INT32, []byte("NULL")),
	}
	parseReplicationApplierLag(&res, row)
	// strconv.NumError will leave ReplicationLagSeconds unset
	assert.Equal(t, uint(0), res.ReplicationLagSeconds)
	row = []sqltypes.Value{
		sqltypes.MakeTrusted(querypb.Type_INT32, []byte("100")),
	}
	parseReplicationApplierLag(&res, row)
	assert.Equal(t, uint(100), res.ReplicationLagSeconds)
}

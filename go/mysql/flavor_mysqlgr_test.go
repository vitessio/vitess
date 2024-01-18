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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql/capabilities"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestMysqlGRParsePrimaryGroupMember(t *testing.T) {
	res := replication.ReplicationStatus{}
	rows := []sqltypes.Value{
		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("host1")),
		sqltypes.MakeTrusted(querypb.Type_INT32, []byte("10")),
	}
	parsePrimaryGroupMember(&res, rows)
	assert.Equal(t, "host1", res.SourceHost)
	assert.Equal(t, int32(10), res.SourcePort)
	assert.Equal(t, replication.ReplicationStateUnknown, res.IOState)
	assert.Equal(t, replication.ReplicationStateUnknown, res.SQLState)
}

func TestMysqlGRReplicationApplierLagParse(t *testing.T) {
	res := replication.ReplicationStatus{}
	row := []sqltypes.Value{
		sqltypes.MakeTrusted(querypb.Type_INT32, []byte("NULL")),
	}
	parseReplicationApplierLag(&res, row)
	// strconv.NumError will leave ReplicationLagSeconds unset
	assert.Equal(t, uint32(0), res.ReplicationLagSeconds)
	row = []sqltypes.Value{
		sqltypes.MakeTrusted(querypb.Type_INT32, []byte("100")),
	}
	parseReplicationApplierLag(&res, row)
	assert.Equal(t, uint32(100), res.ReplicationLagSeconds)
}

func TestMysqlGRSupportCapability(t *testing.T) {
	testcases := []struct {
		version     string
		capability  capabilities.FlavorCapability
		isCapable   bool
		expectError error
	}{
		{
			version:    "8.0.14",
			capability: capabilities.InstantDDLFlavorCapability,
			isCapable:  true,
		},
		{
			version:    "8.0.20",
			capability: capabilities.TransactionalGtidExecutedFlavorCapability,
			isCapable:  true,
		},
		{
			version:    "8.0.0",
			capability: capabilities.InstantAddLastColumnFlavorCapability,
			isCapable:  true,
		},
		{
			version:    "8.0.0",
			capability: capabilities.InstantAddDropColumnFlavorCapability,
			isCapable:  false,
		},
		{
			version:    "5.6.7",
			capability: capabilities.InstantDDLFlavorCapability,
			isCapable:  false,
		},
		{
			version:    "5.7.29",
			capability: capabilities.TransactionalGtidExecutedFlavorCapability,
			isCapable:  false,
		},
		{
			version:    "5.6.7",
			capability: capabilities.MySQLJSONFlavorCapability,
			isCapable:  false,
		},
		{
			version:    "5.7.29",
			capability: capabilities.MySQLJSONFlavorCapability,
			isCapable:  true,
		},
		{
			version:    "8.0.30",
			capability: capabilities.DynamicRedoLogCapacityFlavorCapability,
			isCapable:  true,
		},
		{
			version:    "8.0.29",
			capability: capabilities.DynamicRedoLogCapacityFlavorCapability,
			isCapable:  false,
		},
		{
			version:    "5.7.38",
			capability: capabilities.DynamicRedoLogCapacityFlavorCapability,
			isCapable:  false,
		},
		{
			version:    "8.0.21",
			capability: capabilities.DisableRedoLogFlavorCapability,
			isCapable:  true,
		},
		{
			version:    "8.0.20",
			capability: capabilities.DisableRedoLogFlavorCapability,
			isCapable:  false,
		},
		{
			version:    "8.0.15",
			capability: capabilities.CheckConstraintsCapability,
			isCapable:  false,
		},
		{
			version:    "8.0.20",
			capability: capabilities.CheckConstraintsCapability,
			isCapable:  true,
		},
		{
			version:    "8.0.20-log",
			capability: capabilities.CheckConstraintsCapability,
			isCapable:  true,
		},
		{
			version:    "5.7.38",
			capability: capabilities.PerformanceSchemaDataLocksTableCapability,
			isCapable:  false,
		},
		{
			version:    "8.0.20",
			capability: capabilities.PerformanceSchemaDataLocksTableCapability,
			isCapable:  true,
		},
		{
			// What happens if server version is unspecified
			version:     "",
			capability:  capabilities.CheckConstraintsCapability,
			isCapable:   false,
			expectError: capabilities.ErrUnspecifiedServerVersion,
		},
		{
			// Some ridiculous version. But seeing that we force the flavor to be mysqlGR,
			// then this far futuristic version should actually work.
			version:    "5914.234.17",
			capability: capabilities.CheckConstraintsCapability,
			isCapable:  true,
		},
	}
	for _, tc := range testcases {
		name := fmt.Sprintf("%s %v", tc.version, tc.capability)
		t.Run(name, func(t *testing.T) {
			flavor := &mysqlGRFlavor{mysqlFlavor{serverVersion: tc.version}}
			isCapable, err := flavor.supportsCapability(tc.capability)
			if tc.expectError != nil {
				assert.ErrorContains(t, err, tc.expectError.Error())
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.isCapable, isCapable)
		})
	}
}

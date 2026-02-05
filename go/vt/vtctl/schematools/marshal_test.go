/*
Copyright 2023 The Vitess Authors.

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

package schematools

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sqltypes"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

func TestMarshalResult(t *testing.T) {
	t.Parallel()

	now := time.Now()

	sm := &vtctldatapb.SchemaMigration{
		Uuid:        "abc",
		RequestedAt: protoutil.TimeToProto(now),
		Tablet: &topodatapb.TabletAlias{
			Cell: "zone1",
			Uid:  101,
		},
		Status: vtctldatapb.SchemaMigration_RUNNING,
		Table:  "t1",
	}

	r, err := sqltypes.MarshalResult((*MarshallableSchemaMigration)(sm))
	require.NoError(t, err)
	row := r.Named().Rows[0]

	assert.Equal(t, "abc", row.AsString("migration_uuid", ""))
	assert.Equal(t, now.Format(sqltypes.TimestampFormat), row.AsString("requested_timestamp", ""))
	assert.Equal(t, "zone1-0000000101", row.AsString("tablet", ""))
	assert.Equal(t, "running", row.AsString("status", ""))
	assert.Equal(t, "t1", row.AsString("mysql_table", ""))

	r, err = sqltypes.MarshalResult(MarshallableSchemaMigrations([]*vtctldatapb.SchemaMigration{sm}))
	require.NoError(t, err)
	row = r.Named().Rows[0]

	assert.Equal(t, "abc", row.AsString("migration_uuid", ""))
	assert.Equal(t, now.Format(sqltypes.TimestampFormat), row.AsString("requested_timestamp", ""))
	assert.Equal(t, "zone1-0000000101", row.AsString("tablet", ""))
	assert.Equal(t, "running", row.AsString("status", ""))
	assert.Equal(t, "t1", row.AsString("mysql_table", ""))
}

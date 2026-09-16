/*
Copyright 2026 The Vitess Authors.

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

package mysqlctl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// newReplicationHeartbeatTestDaemon connects a daemon to a fake SQL server.
func newReplicationHeartbeatTestDaemon(t *testing.T, flavor string) (*fakesqldb.DB, MysqlDaemon) {
	t.Helper()

	db := fakesqldb.New(t)
	t.Cleanup(db.Close)
	cp := *db.ConnParams()
	cp.Flavor = flavor
	db.AddQuery("SELECT 1", &sqltypes.Result{})

	mysqld := NewMysqld(dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb"))
	t.Cleanup(mysqld.Close)
	return db, mysqld
}

// TestReplicationHeartbeatReplica checks both values are read with no status queries.
func TestReplicationHeartbeatReplica(t *testing.T) {
	db, mysqld := newReplicationHeartbeatTestDaemon(t, "")
	db.AddQuery("SELECT * FROM performance_schema.replication_connection_configuration", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("HEARTBEAT_INTERVAL", "float64"), "4.5",
	))
	db.AddQuery("SELECT @@global.replica_net_timeout", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("replica_net_timeout", "int32"), "9",
	))

	interval, netTimeout, err := mysqld.ReplicationHeartbeat(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 4.5, interval)
	assert.Equal(t, int32(9), netTimeout)
	assert.Equal(t, 1, db.GetQueryCalledNum("SELECT * FROM performance_schema.replication_connection_configuration"))
	assert.Equal(t, 1, db.GetQueryCalledNum("SELECT @@global.replica_net_timeout"))
}

// TestReplicationHeartbeatNotReplica checks that a primary returns ErrNotReplica without a timeout query.
func TestReplicationHeartbeatNotReplica(t *testing.T) {
	db, mysqld := newReplicationHeartbeatTestDaemon(t, "")
	db.AddQuery("SELECT * FROM performance_schema.replication_connection_configuration", &sqltypes.Result{})

	_, _, err := mysqld.ReplicationHeartbeat(t.Context())
	require.ErrorIs(t, err, mysql.ErrNotReplica)
}

// TestReplicationHeartbeatUnsupportedFlavor checks that FilePos returns ErrNotReplica with no queries.
func TestReplicationHeartbeatUnsupportedFlavor(t *testing.T) {
	_, mysqld := newReplicationHeartbeatTestDaemon(t, replication.FilePosFlavorID)

	_, _, err := mysqld.ReplicationHeartbeat(t.Context())
	require.ErrorIs(t, err, mysql.ErrNotReplica)
}

// TestReplicationHeartbeatQueryError checks errors from each query.
func TestReplicationHeartbeatQueryError(t *testing.T) {
	for _, query := range []string{
		"SELECT * FROM performance_schema.replication_connection_configuration",
		"SELECT @@global.replica_net_timeout",
	} {
		t.Run(query, func(t *testing.T) {
			db, mysqld := newReplicationHeartbeatTestDaemon(t, "")
			db.AddQuery("SELECT * FROM performance_schema.replication_connection_configuration", sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("HEARTBEAT_INTERVAL", "float64"), "4.5",
			))
			db.AddRejectedQuery(query, vterrors.New(vtrpcpb.Code_INTERNAL, "configuration query failed"))

			_, _, err := mysqld.ReplicationHeartbeat(t.Context())
			require.ErrorContains(t, err, "configuration query failed")
		})
	}
}

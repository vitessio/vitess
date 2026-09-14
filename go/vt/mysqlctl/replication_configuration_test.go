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

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// newReplicationConfigurationTestDaemon connects a daemon to a fake SQL server.
func newReplicationConfigurationTestDaemon(t *testing.T, flavor string) (*fakesqldb.DB, MysqlDaemon) {
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

// TestReplicationConfigurationReplica checks both settings with no status queries.
func TestReplicationConfigurationReplica(t *testing.T) {
	db, mysqld := newReplicationConfigurationTestDaemon(t, "")
	db.AddQuery("SELECT * FROM performance_schema.replication_connection_configuration", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("HEARTBEAT_INTERVAL", "float64"), "4.5",
	))
	db.AddQuery("SELECT @@global.replica_net_timeout", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("replica_net_timeout", "int32"), "9",
	))

	configuration, err := mysqld.ReplicationConfiguration(t.Context())
	require.NoError(t, err)
	require.NotNil(t, configuration)
	assert.Equal(t, 4.5, configuration.HeartbeatInterval)
	assert.Equal(t, int32(9), configuration.ReplicaNetTimeout)
	assert.Equal(t, 1, db.GetQueryCalledNum("SELECT * FROM performance_schema.replication_connection_configuration"))
	assert.Equal(t, 1, db.GetQueryCalledNum("SELECT @@global.replica_net_timeout"))
}

// TestReplicationConfigurationNotReplica checks that an empty result needs no timeout query.
func TestReplicationConfigurationNotReplica(t *testing.T) {
	db, mysqld := newReplicationConfigurationTestDaemon(t, "")
	db.AddQuery("SELECT * FROM performance_schema.replication_connection_configuration", &sqltypes.Result{})

	configuration, err := mysqld.ReplicationConfiguration(t.Context())
	require.NoError(t, err)
	assert.Nil(t, configuration)
}

// TestReplicationConfigurationUnsupportedFlavor checks that FilePos needs no configuration queries.
func TestReplicationConfigurationUnsupportedFlavor(t *testing.T) {
	_, mysqld := newReplicationConfigurationTestDaemon(t, replication.FilePosFlavorID)

	configuration, err := mysqld.ReplicationConfiguration(t.Context())
	require.NoError(t, err)
	assert.Nil(t, configuration)
}

// TestReplicationConfigurationQueryError checks errors from each configuration query.
func TestReplicationConfigurationQueryError(t *testing.T) {
	for _, query := range []string{
		"SELECT * FROM performance_schema.replication_connection_configuration",
		"SELECT @@global.replica_net_timeout",
	} {
		t.Run(query, func(t *testing.T) {
			db, mysqld := newReplicationConfigurationTestDaemon(t, "")
			db.AddQuery("SELECT * FROM performance_schema.replication_connection_configuration", sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("HEARTBEAT_INTERVAL", "float64"), "4.5",
			))
			db.AddRejectedQuery(query, vterrors.New(vtrpcpb.Code_INTERNAL, "configuration query failed"))

			configuration, err := mysqld.ReplicationConfiguration(t.Context())
			require.ErrorContains(t, err, "configuration query failed")
			assert.Nil(t, configuration)
		})
	}
}

// TestFakeMysqlDaemonReplicationConfiguration checks the default and selected results.
func TestFakeMysqlDaemonReplicationConfiguration(t *testing.T) {
	fake := NewFakeMysqlDaemon(nil)
	var mysqld MysqlDaemon = fake

	configuration, err := mysqld.ReplicationConfiguration(t.Context())
	require.NoError(t, err)
	assert.Nil(t, configuration)

	fake.ReplicationConfigurationResult = &replicationdatapb.Configuration{HeartbeatInterval: 4.5, ReplicaNetTimeout: 9}
	configuration, err = mysqld.ReplicationConfiguration(t.Context())
	require.NoError(t, err)
	assert.Equal(t, fake.ReplicationConfigurationResult, configuration)

	fake.ReplicationConfigurationResult = nil
	fake.ReplicationConfigurationError = vterrors.New(vtrpcpb.Code_INTERNAL, "configuration unavailable")
	configuration, err = mysqld.ReplicationConfiguration(t.Context())
	require.ErrorIs(t, err, fake.ReplicationConfigurationError)
	assert.Nil(t, configuration)
}

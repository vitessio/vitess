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

package inst

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	primaryServerUUID = "aaaaaaaa-1111-1111-1111-aaaaaaaaaaaa"
	replicaServerUUID = "bbbbbbbb-2222-2222-2222-bbbbbbbbbbbb"
)

func TestInstanceFromFullStatus(t *testing.T) {
	primaryTablet := &topodatapb.Tablet{
		Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		MysqlHostname: "localhost",
		MysqlPort:     17100,
		Type:          topodatapb.TabletType_PRIMARY,
	}
	replicaTablet := &topodatapb.Tablet{
		Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
		MysqlHostname: "localhost",
		MysqlPort:     17101,
		Type:          topodatapb.TabletType_REPLICA,
	}

	primaryStatus := &replicationdatapb.FullStatus{
		ServerId:                    1001,
		ServerUuid:                  primaryServerUUID,
		TabletType:                  topodatapb.TabletType_PRIMARY,
		Version:                     "8.0.40",
		VersionComment:              "MySQL Community Server - GPL",
		ReadOnly:                    false,
		LogBinEnabled:               true,
		LogReplicaUpdates:           true,
		BinlogFormat:                "ROW",
		GtidMode:                    "ON",
		BinlogRowImage:              "FULL",
		GtidPurged:                  "",
		SemiSyncPrimaryEnabled:      true,
		SemiSyncReplicaEnabled:      true,
		SemiSyncPrimaryStatus:       true,
		SemiSyncReplicaStatus:       false,
		SemiSyncBlocked:             false,
		SemiSyncPrimaryClients:      2,
		SemiSyncWaitForReplicaCount: 1,
		SemiSyncPrimaryTimeout:      1000000000000000000,
		PrimaryStatus: &replicationdatapb.PrimaryStatus{
			Position:     "MySQL56/" + primaryServerUUID + ":1-100",
			FilePosition: "FilePos/vt-0000000100-bin.000001:1307",
		},
	}

	replicaStatus := &replicationdatapb.FullStatus{
		ServerId:                    1002,
		ServerUuid:                  replicaServerUUID,
		TabletType:                  topodatapb.TabletType_REPLICA,
		Version:                     "8.0.40",
		VersionComment:              "MySQL Community Server - GPL",
		ReadOnly:                    true,
		LogBinEnabled:               true,
		LogReplicaUpdates:           true,
		BinlogFormat:                "ROW",
		GtidMode:                    "ON",
		BinlogRowImage:              "FULL",
		GtidPurged:                  "",
		SemiSyncPrimaryEnabled:      false,
		SemiSyncReplicaEnabled:      true,
		SemiSyncPrimaryStatus:       false,
		SemiSyncReplicaStatus:       true,
		SemiSyncBlocked:             false,
		SemiSyncPrimaryClients:      0,
		SemiSyncWaitForReplicaCount: 1,
		SemiSyncPrimaryTimeout:      1000000000000000000,
		PrimaryStatus: &replicationdatapb.PrimaryStatus{
			Position:     "MySQL56/" + primaryServerUUID + ":1-100," + replicaServerUUID + ":1-1",
			FilePosition: "FilePos/vt-0000000101-bin.000001:2000",
		},
		ReplicationStatus: &replicationdatapb.Status{
			SourceUser:                             "vt_repl",
			IoState:                                3,
			SqlState:                               3,
			RelayLogSourceBinlogEquivalentPosition: "FilePos/vt-0000000100-bin.000001:1307",
			FilePosition:                           "FilePos/vt-0000000100-bin.000001:1307",
			RelayLogFilePosition:                   "FilePos/vt-0000000101-relay.000002:1500",
			LastSqlError:                           "",
			LastIoError:                            "",
			SqlDelay:                               0,
			AutoPosition:                           true,
			SourceUuid:                             primaryServerUUID,
			HasReplicationFilters:                  false,
			SourceHost:                             "localhost",
			SourcePort:                             17100,
			ReplicationLagUnknown:                  false,
			ReplicationLagSeconds:                  0,
			SslAllowed:                             false,
		},
		ReplicationConfiguration: &replicationdatapb.Configuration{
			HeartbeatInterval: 4.0,
			ReplicaNetTimeout: 8,
		},
	}

	t.Run("primary", func(t *testing.T) {
		instance, errs := instanceFromFullStatus(primaryTablet, primaryStatus)
		for _, err := range errs {
			require.NoError(t, err)
		}

		assert.Equal(t, "localhost", instance.Hostname)
		assert.Equal(t, 17100, instance.Port)
		assert.Equal(t, topodatapb.TabletType_PRIMARY, instance.TabletType)
		assert.EqualValues(t, 1001, instance.ServerID)
		assert.Equal(t, primaryServerUUID, instance.ServerUUID)
		assert.Equal(t, "8.0.40", instance.Version)
		assert.Equal(t, "MySQL Community Server - GPL", instance.VersionComment)
		assert.False(t, instance.ReadOnly)
		assert.True(t, instance.LogBinEnabled)
		assert.True(t, instance.LogReplicationUpdatesEnabled)
		assert.Equal(t, "ROW", instance.BinlogFormat)
		assert.Equal(t, "ON", instance.GTIDMode)
		assert.Equal(t, "FULL", instance.BinlogRowImage)
		assert.True(t, instance.SupportsOracleGTID)
		assert.Equal(t, "vt-0000000100-bin.000001", instance.SelfBinlogCoordinates.LogFile)
		assert.EqualValues(t, 1307, instance.SelfBinlogCoordinates.LogPos)
		assert.True(t, instance.SemiSyncPrimaryEnabled)
		assert.True(t, instance.SemiSyncReplicaEnabled)
		assert.True(t, instance.SemiSyncPrimaryStatus)
		assert.False(t, instance.SemiSyncReplicaStatus)
		assert.False(t, instance.SemiSyncBlocked)
		assert.EqualValues(t, 2, instance.SemiSyncPrimaryClients)
		assert.EqualValues(t, 1, instance.SemiSyncPrimaryWaitForReplicaCount)
		assert.EqualValues(t, 1000000000000000000, instance.SemiSyncPrimaryTimeout)
		assert.Equal(t, primaryServerUUID+":1-100", instance.ExecutedGtidSet)
		assert.Empty(t, instance.GtidPurged)
		assert.False(t, instance.HasReplicationCredentials)
		assert.Equal(t, ReplicationThreadStateNoThread, instance.ReplicationIOThreadState)
		assert.Equal(t, ReplicationThreadStateNoThread, instance.ReplicationSQLThreadState)
		assert.False(t, instance.ReplicationIOThreadRuning)
		assert.False(t, instance.ReplicationSQLThreadRuning)
		assert.EqualValues(t, 0, instance.HeartbeatInterval)
		assert.EqualValues(t, 0, instance.ReplicaNetTimeout)
	})

	t.Run("replica", func(t *testing.T) {
		instance, errs := instanceFromFullStatus(replicaTablet, replicaStatus)
		for _, err := range errs {
			require.NoError(t, err)
		}

		assert.Equal(t, "localhost", instance.Hostname)
		assert.Equal(t, 17101, instance.Port)
		assert.Equal(t, topodatapb.TabletType_REPLICA, instance.TabletType)
		assert.EqualValues(t, 1002, instance.ServerID)
		assert.Equal(t, replicaServerUUID, instance.ServerUUID)
		assert.Equal(t, "8.0.40", instance.Version)
		assert.Equal(t, "MySQL Community Server - GPL", instance.VersionComment)
		assert.True(t, instance.ReadOnly)
		assert.True(t, instance.LogBinEnabled)
		assert.True(t, instance.LogReplicationUpdatesEnabled)
		assert.Equal(t, "ROW", instance.BinlogFormat)
		assert.Equal(t, "ON", instance.GTIDMode)
		assert.Equal(t, "FULL", instance.BinlogRowImage)
		assert.True(t, instance.SupportsOracleGTID)
		assert.Equal(t, "localhost", instance.SourceHost)
		assert.Equal(t, 17100, instance.SourcePort)
		assert.Equal(t, "vt-0000000101-bin.000001", instance.SelfBinlogCoordinates.LogFile)
		assert.EqualValues(t, 2000, instance.SelfBinlogCoordinates.LogPos)
		assert.False(t, instance.SemiSyncPrimaryEnabled)
		assert.True(t, instance.SemiSyncReplicaEnabled)
		assert.False(t, instance.SemiSyncPrimaryStatus)
		assert.True(t, instance.SemiSyncReplicaStatus)
		assert.False(t, instance.SemiSyncBlocked)
		assert.EqualValues(t, 0, instance.SemiSyncPrimaryClients)
		assert.EqualValues(t, 1, instance.SemiSyncPrimaryWaitForReplicaCount)
		assert.EqualValues(t, 1000000000000000000, instance.SemiSyncPrimaryTimeout)
		assert.Equal(t, primaryServerUUID+":1-100,"+replicaServerUUID+":1", instance.ExecutedGtidSet)
		assert.Empty(t, instance.GtidPurged)
		assert.True(t, instance.HasReplicationCredentials)
		assert.Equal(t, ReplicationThreadStateRunning, instance.ReplicationIOThreadState)
		assert.Equal(t, ReplicationThreadStateRunning, instance.ReplicationSQLThreadState)
		assert.True(t, instance.ReplicationIOThreadRuning)
		assert.True(t, instance.ReplicationSQLThreadRuning)
		assert.Equal(t, "vt-0000000100-bin.000001", instance.ReadBinlogCoordinates.LogFile)
		assert.EqualValues(t, 1307, instance.ReadBinlogCoordinates.LogPos)
		assert.Equal(t, "vt-0000000100-bin.000001", instance.ExecBinlogCoordinates.LogFile)
		assert.EqualValues(t, 1307, instance.ExecBinlogCoordinates.LogPos)
		assert.False(t, instance.IsDetached)
		assert.Equal(t, "vt-0000000101-relay.000002", instance.RelaylogCoordinates.LogFile)
		assert.EqualValues(t, 1500, instance.RelaylogCoordinates.LogPos)
		assert.Equal(t, RelayLog, instance.RelaylogCoordinates.Type)
		assert.Empty(t, instance.LastIOError)
		assert.Empty(t, instance.LastSQLError)
		assert.EqualValues(t, 0, instance.SQLDelay)
		assert.True(t, instance.UsingOracleGTID)
		assert.Equal(t, primaryServerUUID, instance.SourceUUID)
		assert.False(t, instance.HasReplicationFilters)
		assert.True(t, instance.SecondsBehindPrimary.Valid)
		assert.EqualValues(t, 0, instance.SecondsBehindPrimary.Int64)
		assert.False(t, instance.AllowTLS)
		assert.EqualValues(t, 4.0, instance.HeartbeatInterval)
		assert.EqualValues(t, 8, instance.ReplicaNetTimeout)
	})
}

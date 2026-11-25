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
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtorc/db"
)

func TestOperationalStateChanged(t *testing.T) {
	const lagThreshold int64 = 10

	newBaseline := func() *Instance {
		return &Instance{
			Hostname:                           "tablet-1",
			Port:                               3306,
			TabletType:                         topodatapb.TabletType_REPLICA,
			Cell:                               "zone1",
			ServerID:                           100,
			ServerUUID:                         "11111111-1111-1111-1111-111111111111",
			Version:                            "8.0.36",
			VersionComment:                     "MySQL Community Server",
			ReadOnly:                           true,
			BinlogFormat:                       "ROW",
			BinlogRowImage:                     "FULL",
			LogBinEnabled:                      true,
			LogReplicationUpdatesEnabled:       true,
			SourceHost:                         "primary-1",
			SourcePort:                         3306,
			SourceUUID:                         "22222222-2222-2222-2222-222222222222",
			ReplicaNetTimeout:                  60,
			HeartbeatInterval:                  1.5,
			ReplicationSQLThreadRuning:         true,
			ReplicationIOThreadRuning:          true,
			ReplicationSQLThreadState:          ReplicationThreadStateRunning,
			ReplicationIOThreadState:           ReplicationThreadStateRunning,
			HasReplicationFilters:              false,
			GTIDMode:                           "ON",
			SupportsOracleGTID:                 true,
			UsingOracleGTID:                    true,
			IsDetached:                         false,
			SecondsBehindPrimary:               sql.NullInt64{Int64: 1, Valid: true},
			ReplicationLagSeconds:              sql.NullInt64{Int64: 1, Valid: true},
			SQLDelay:                           0,
			ExecutedGtidSet:                    "22222222-2222-2222-2222-222222222222:1-100",
			GtidPurged:                         "22222222-2222-2222-2222-222222222222:1-10",
			GtidErrant:                         "",
			IsCoPrimary:                        false,
			HasReplicationCredentials:          true,
			SemiSyncEnforced:                   true,
			SemiSyncPrimaryEnabled:             false,
			SemiSyncReplicaEnabled:             true,
			SemiSyncPrimaryTimeout:             10000,
			SemiSyncPrimaryWaitForReplicaCount: 1,
			SemiSyncPrimaryStatus:              false,
			SemiSyncPrimaryClients:             0,
			SemiSyncReplicaStatus:              true,
			SemiSyncBlocked:                    false,
			LastSeenTimestamp:                  "2026-01-01 00:00:00",
			IsLastCheckValid:                   true,
			IsUpToDate:                         true,
			IsRecentlyChecked:                  true,
			SecondsSinceLastSeen:               sql.NullInt64{Int64: 5, Valid: true},
			AllowTLS:                           true,
			LastDiscoveryLatency:               10 * time.Millisecond,
			SelfBinlogCoordinates:              BinlogCoordinates{LogFile: "mysql-bin.000001", LogPos: 100},
			ReadBinlogCoordinates:              BinlogCoordinates{LogFile: "mysql-bin.000001", LogPos: 100},
			ExecBinlogCoordinates:              BinlogCoordinates{LogFile: "mysql-bin.000001", LogPos: 100},
			RelaylogCoordinates:                BinlogCoordinates{LogFile: "relay-bin.000001", LogPos: 100},
		}
	}

	t.Run("steady state returns false", func(t *testing.T) {
		prev := newBaseline()
		curr := newBaseline()

		curr.InstanceAlias = &topodatapb.TabletAlias{Cell: "zone1", Uid: 999}
		curr.FlavorName = "MySQL"
		curr.ExecutedGtidSet = "22222222-2222-2222-2222-222222222222:1-250"
		curr.GtidPurged = "22222222-2222-2222-2222-222222222222:1-50"
		curr.SelfBinlogCoordinates = BinlogCoordinates{LogFile: "mysql-bin.000002", LogPos: 999}
		curr.ReadBinlogCoordinates = BinlogCoordinates{LogFile: "mysql-bin.000002", LogPos: 999}
		curr.ExecBinlogCoordinates = BinlogCoordinates{LogFile: "mysql-bin.000002", LogPos: 999}
		curr.RelaylogCoordinates = BinlogCoordinates{LogFile: "relay-bin.000002", LogPos: 999}
		curr.SemiSyncEnforced = false
		curr.LastDiscoveryLatency = 42 * time.Millisecond
		curr.LastSeenTimestamp = "2026-01-01 00:00:05"
		curr.IsLastCheckValid = false
		curr.IsUpToDate = false
		curr.IsRecentlyChecked = false
		curr.SecondsSinceLastSeen = sql.NullInt64{Int64: 0, Valid: true}
		curr.SecondsBehindPrimary = sql.NullInt64{Int64: 5, Valid: true}
		curr.ReplicationLagSeconds = sql.NullInt64{Int64: 5, Valid: true}
		curr.StalledDisk = true
		curr.Problems = []string{"not_recently_checked"}

		assert.False(t, curr.OperationalStateChanged(prev, lagThreshold))
	})

	t.Run("lag jitter within threshold returns false", func(t *testing.T) {
		prev := newBaseline()
		curr := newBaseline()
		prev.SecondsBehindPrimary = sql.NullInt64{Int64: 0, Valid: true}
		curr.SecondsBehindPrimary = sql.NullInt64{Int64: lagThreshold, Valid: true}

		assert.False(t, curr.OperationalStateChanged(prev, lagThreshold))
	})

	t.Run("lag within SQLDelay tolerance returns false", func(t *testing.T) {
		prev := newBaseline()
		curr := newBaseline()
		prev.SQLDelay = 60
		curr.SQLDelay = 60
		prev.SecondsBehindPrimary = sql.NullInt64{Int64: 60, Valid: true}
		curr.SecondsBehindPrimary = sql.NullInt64{Int64: 60 + lagThreshold, Valid: true}

		assert.False(t, curr.OperationalStateChanged(prev, lagThreshold))
	})

	t.Run("lag exceeding SQLDelay tolerance returns true", func(t *testing.T) {
		prev := newBaseline()
		curr := newBaseline()
		prev.SQLDelay = 60
		curr.SQLDelay = 60
		prev.SecondsBehindPrimary = sql.NullInt64{Int64: 60, Valid: true}
		curr.SecondsBehindPrimary = sql.NullInt64{Int64: 60 + lagThreshold + 1, Valid: true}

		assert.True(t, curr.OperationalStateChanged(prev, lagThreshold))
	})

	t.Run("lag below SQLDelay beyond threshold returns true", func(t *testing.T) {
		prev := newBaseline()
		curr := newBaseline()
		prev.SQLDelay = 60
		curr.SQLDelay = 60
		prev.SecondsBehindPrimary = sql.NullInt64{Int64: 60, Valid: true}
		curr.SecondsBehindPrimary = sql.NullInt64{Int64: 60 - lagThreshold - 1, Valid: true}

		assert.True(t, curr.OperationalStateChanged(prev, lagThreshold))
	})

	t.Run("invalid lag ignores stale Int64 values", func(t *testing.T) {
		prev := newBaseline()
		curr := newBaseline()
		prev.SecondsBehindPrimary = sql.NullInt64{Int64: 0, Valid: false}
		curr.SecondsBehindPrimary = sql.NullInt64{Int64: lagThreshold + 50, Valid: false}

		assert.False(t, curr.OperationalStateChanged(prev, lagThreshold))
	})

	t.Run("both lagging stays quiet on steady lag", func(t *testing.T) {
		prev := newBaseline()
		curr := newBaseline()
		prev.SecondsBehindPrimary = sql.NullInt64{Int64: 25, Valid: true}
		curr.SecondsBehindPrimary = sql.NullInt64{Int64: 30, Valid: true}

		assert.False(t, curr.OperationalStateChanged(prev, lagThreshold))
	})

	t.Run("nil receiver or prev returns true", func(t *testing.T) {
		curr := newBaseline()
		var nilInstance *Instance

		assert.True(t, curr.OperationalStateChanged(nil, lagThreshold), "nil prev")
		assert.True(t, nilInstance.OperationalStateChanged(curr, lagThreshold), "nil curr")
	})

	t.Run("persisted instance matches live state", func(t *testing.T) {
		t.Cleanup(func() { db.ClearVTOrcDatabase() })

		replica := newBaseline()
		replica.InstanceAlias = &topodatapb.TabletAlias{Cell: "zone1", Uid: 501}
		replica.AncestryUUID = "22222222-2222-2222-2222-222222222222,11111111-1111-1111-1111-111111111111"
		replica.ReplicationDepth = 1
		replica.LastSQLError = "Duplicate entry"
		replica.LastIOError = "Lost connection"
		replica.HasReplicationFilters = true
		replica.IsDetached = true
		replica.ExecBinlogCoordinates = BinlogCoordinates{LogFile: "//mysql-bin.000001:1234", LogPos: 100}
		replica.SQLDelay = 60
		replica.SecondsBehindPrimary = sql.NullInt64{Int64: 62, Valid: true}
		replica.ReplicationLagSeconds = sql.NullInt64{Int64: 62, Valid: true}
		replica.GtidErrant = "11111111-1111-1111-1111-111111111111:1"
		replica.IsCoPrimary = true
		replica.SemiSyncPrimaryEnabled = true
		replica.SemiSyncPrimaryTimeout = 1000000000000000000
		replica.SemiSyncPrimaryWaitForReplicaCount = 2
		replica.SemiSyncPrimaryStatus = true
		replica.SemiSyncPrimaryClients = 2
		replica.SemiSyncBlocked = true

		primary := &Instance{
			InstanceAlias:             &topodatapb.TabletAlias{Cell: "zone1", Uid: 502},
			Hostname:                  "primary-1",
			Port:                      3306,
			TabletType:                topodatapb.TabletType_PRIMARY,
			Cell:                      "zone1",
			ServerID:                  502,
			ServerUUID:                "33333333-3333-3333-3333-333333333333",
			Version:                   "8.0.36",
			BinlogFormat:              "ROW",
			LogBinEnabled:             true,
			GTIDMode:                  "ON",
			SupportsOracleGTID:        true,
			ReplicationIOThreadState:  ReplicationThreadStateNoThread,
			ReplicationSQLThreadState: ReplicationThreadStateNoThread,
			SecondsBehindPrimary:      sql.NullInt64{Valid: false},
		}

		for _, curr := range []*Instance{replica, primary} {
			require.NoError(t, SaveTablet(&topodatapb.Tablet{
				Alias:         curr.InstanceAlias,
				MysqlHostname: curr.Hostname,
				MysqlPort:     int32(curr.Port),
				Keyspace:      "ks",
				Shard:         "0",
				Type:          curr.TabletType,
			}))
			require.NoError(t, WriteInstance(curr, true, nil))

			prev, found, err := ReadInstance(curr.InstanceAlias)
			require.NoError(t, err)
			require.True(t, found)
			assert.False(t, curr.OperationalStateChanged(prev, lagThreshold))
		}
	})

	transitions := []struct {
		name   string
		field  string
		mutate func(i *Instance)
	}{
		{name: "tablet type", field: "TabletType", mutate: func(i *Instance) { i.TabletType = topodatapb.TabletType_PRIMARY }},
		{name: "read only", field: "ReadOnly", mutate: func(i *Instance) { i.ReadOnly = false }},
		{name: "hostname", field: "Hostname", mutate: func(i *Instance) { i.Hostname = "tablet-2" }},
		{name: "port", field: "Port", mutate: func(i *Instance) { i.Port = 3307 }},
		{name: "cell", field: "Cell", mutate: func(i *Instance) { i.Cell = "zone2" }},
		{name: "server id", field: "ServerID", mutate: func(i *Instance) { i.ServerID = 101 }},
		{name: "server uuid", field: "ServerUUID", mutate: func(i *Instance) { i.ServerUUID = "33333333-3333-3333-3333-333333333333" }},
		{name: "source host", field: "SourceHost", mutate: func(i *Instance) { i.SourceHost = "primary-2" }},
		{name: "source port", field: "SourcePort", mutate: func(i *Instance) { i.SourcePort = 3307 }},
		{name: "source uuid", field: "SourceUUID", mutate: func(i *Instance) { i.SourceUUID = "44444444-4444-4444-4444-444444444444" }},
		{name: "ancestry uuid", field: "AncestryUUID", mutate: func(i *Instance) {
			i.AncestryUUID = "44444444-4444-4444-4444-444444444444,11111111-1111-1111-1111-111111111111"
		}},
		{name: "replication depth", field: "ReplicationDepth", mutate: func(i *Instance) { i.ReplicationDepth = 2 }},
		{name: "is detached", field: "IsDetached", mutate: func(i *Instance) { i.IsDetached = true }},
		{name: "is co-primary", field: "IsCoPrimary", mutate: func(i *Instance) { i.IsCoPrimary = true }},
		{name: "replication sql thread running", field: "ReplicationSQLThreadRuning", mutate: func(i *Instance) { i.ReplicationSQLThreadRuning = false }},
		{name: "replication io thread running", field: "ReplicationIOThreadRuning", mutate: func(i *Instance) { i.ReplicationIOThreadRuning = false }},
		{name: "replication sql thread state", field: "ReplicationSQLThreadState", mutate: func(i *Instance) { i.ReplicationSQLThreadState = ReplicationThreadStateStopped }},
		{name: "replication io thread state", field: "ReplicationIOThreadState", mutate: func(i *Instance) { i.ReplicationIOThreadState = ReplicationThreadStateStopped }},
		{name: "last sql error", field: "LastSQLError", mutate: func(i *Instance) { i.LastSQLError = "Duplicate entry" }},
		{name: "last io error", field: "LastIOError", mutate: func(i *Instance) { i.LastIOError = "Lost connection" }},
		{name: "errant gtid", field: "GtidErrant", mutate: func(i *Instance) { i.GtidErrant = "11111111-1111-1111-1111-111111111111:1" }},
		{name: "replication filters", field: "HasReplicationFilters", mutate: func(i *Instance) { i.HasReplicationFilters = true }},
		{name: "replication credentials", field: "HasReplicationCredentials", mutate: func(i *Instance) { i.HasReplicationCredentials = false }},
		{name: "sql delay", field: "SQLDelay", mutate: func(i *Instance) { i.SQLDelay = 5 }},
		{name: "lag exceeds threshold", field: "SecondsBehindPrimary", mutate: func(i *Instance) { i.SecondsBehindPrimary = sql.NullInt64{Int64: lagThreshold + 1, Valid: true} }},
		{name: "lag validity changed", field: "SecondsBehindPrimary", mutate: func(i *Instance) { i.SecondsBehindPrimary = sql.NullInt64{Valid: false} }},
		{name: "semi-sync primary enabled", field: "SemiSyncPrimaryEnabled", mutate: func(i *Instance) { i.SemiSyncPrimaryEnabled = true }},
		{name: "semi-sync replica enabled", field: "SemiSyncReplicaEnabled", mutate: func(i *Instance) { i.SemiSyncReplicaEnabled = false }},
		{name: "semi-sync primary timeout", field: "SemiSyncPrimaryTimeout", mutate: func(i *Instance) { i.SemiSyncPrimaryTimeout = 5000 }},
		{name: "semi-sync wait count", field: "SemiSyncPrimaryWaitForReplicaCount", mutate: func(i *Instance) { i.SemiSyncPrimaryWaitForReplicaCount = 2 }},
		{name: "semi-sync primary status", field: "SemiSyncPrimaryStatus", mutate: func(i *Instance) { i.SemiSyncPrimaryStatus = true }},
		{name: "semi-sync primary clients", field: "SemiSyncPrimaryClients", mutate: func(i *Instance) { i.SemiSyncPrimaryClients = 2 }},
		{name: "semi-sync replica status", field: "SemiSyncReplicaStatus", mutate: func(i *Instance) { i.SemiSyncReplicaStatus = false }},
		{name: "semi-sync blocked", field: "SemiSyncBlocked", mutate: func(i *Instance) { i.SemiSyncBlocked = true }},
		{name: "gtid mode", field: "GTIDMode", mutate: func(i *Instance) { i.GTIDMode = "OFF" }},
		{name: "supports oracle gtid", field: "SupportsOracleGTID", mutate: func(i *Instance) { i.SupportsOracleGTID = false }},
		{name: "using oracle gtid", field: "UsingOracleGTID", mutate: func(i *Instance) { i.UsingOracleGTID = false }},
		{name: "log bin enabled", field: "LogBinEnabled", mutate: func(i *Instance) { i.LogBinEnabled = false }},
		{name: "log replication updates", field: "LogReplicationUpdatesEnabled", mutate: func(i *Instance) { i.LogReplicationUpdatesEnabled = false }},
		{name: "binlog format", field: "BinlogFormat", mutate: func(i *Instance) { i.BinlogFormat = "STATEMENT" }},
		{name: "binlog row image", field: "BinlogRowImage", mutate: func(i *Instance) { i.BinlogRowImage = "MINIMAL" }},
		{name: "replica net timeout", field: "ReplicaNetTimeout", mutate: func(i *Instance) { i.ReplicaNetTimeout = 30 }},
		{name: "heartbeat interval", field: "HeartbeatInterval", mutate: func(i *Instance) { i.HeartbeatInterval = 2.0 }},
		{name: "allow tls", field: "AllowTLS", mutate: func(i *Instance) { i.AllowTLS = false }},
		{name: "version", field: "Version", mutate: func(i *Instance) { i.Version = "8.0.40" }},
		{name: "version comment", field: "VersionComment", mutate: func(i *Instance) { i.VersionComment = "Percona Server" }},
	}

	for _, tc := range transitions {
		t.Run(tc.name, func(t *testing.T) {
			prev := newBaseline()
			curr := newBaseline()
			tc.mutate(curr)

			assert.True(t, curr.OperationalStateChanged(prev, lagThreshold), "change")
			assert.True(t, prev.OperationalStateChanged(curr, lagThreshold), "clear")
		})
	}

	t.Run("every Instance field is classified as compared or ignored", func(t *testing.T) {
		comparedFields := make(map[string]bool, len(transitions))
		for _, tc := range transitions {
			comparedFields[tc.field] = true
		}

		// Classify every excluded Instance field explicitly. Adding a new
		// exported field to Instance must fail this test until classified.
		ignoredFields := map[string]bool{
			"InstanceAlias":         true, // Primary key of the tablet being polled.
			"FlavorName":            true, // Derived from VersionComment on DB read only.
			"SelfBinlogCoordinates": true, // Advances on every write.
			"ReadBinlogCoordinates": true, // Advances on every write.
			"ExecBinlogCoordinates": true, // Advances on every write.
			"RelaylogCoordinates":   true, // Advances on every write.
			"ExecutedGtidSet":       true, // Advances on every write.
			"GtidPurged":            true, // Advances on binlog purge.
			"ReplicationLagSeconds": true, // Mirrors SecondsBehindPrimary.
			"SemiSyncEnforced":      true, // Unused field never populated during discovery.
			"LastSeenTimestamp":     true, // Advances on every poll.
			"IsLastCheckValid":      true, // Handled by caller lifecycle check.
			"IsUpToDate":            true, // Differs between stale prev and fresh curr on every poll.
			"IsRecentlyChecked":     true, // Poll-age metadata.
			"SecondsSinceLastSeen":  true, // Advances with wall clock.
			"StalledDisk":           true, // Not populated on readInstanceRow; stalled disk returns nil instance.
			"Problems":              true, // Derived on DB read only, empty on fresh poll.
			"LastDiscoveryLatency":  true, // Per-poll timing measurement.
		}

		instanceType := reflect.TypeFor[Instance]()
		seen := make(map[string]bool, instanceType.NumField())
		for field := range instanceType.Fields() {
			if !field.IsExported() {
				continue
			}

			seen[field.Name] = true
			inCompared := comparedFields[field.Name]
			inIgnored := ignoredFields[field.Name]
			require.False(t, inCompared && inIgnored, "field %s is listed in both compared and ignored", field.Name)
			assert.True(t, inCompared || inIgnored, "Instance.%s is not classified in OperationalStateChanged tests; add a transition case or mark it ignored", field.Name)
		}

		for fieldName := range comparedFields {
			assert.True(t, seen[fieldName], "compared field %s does not exist on Instance", fieldName)
		}

		for fieldName := range ignoredFields {
			assert.True(t, seen[fieldName], "ignored field %s does not exist on Instance", fieldName)
		}
	})
}

package inst

import (
	"bytes"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
)

var (
	spacesRegexp = regexp.MustCompile(`[ \t\n\r]+`)
)

func normalizeQuery(name string) string {
	name = strings.ReplaceAll(name, "`", "")
	name = spacesRegexp.ReplaceAllString(name, " ")
	name = strings.TrimSpace(name)
	return name
}

func stripSpaces(s string) string {
	s = spacesRegexp.ReplaceAllString(s, "")
	return s
}

func mkTestInstances() []*Instance {
	i710 := Instance{InstanceAlias: "zone1-i710", Hostname: "i710", Port: 3306, Cell: "zone1", TabletType: topodatapb.TabletType_PRIMARY, ServerID: 710, ExecBinlogCoordinates: BinlogCoordinates{LogFile: "mysql.000007", LogPos: 10}}
	i720 := Instance{InstanceAlias: "zone1-i720", Hostname: "i720", Port: 3306, Cell: "zone1", TabletType: topodatapb.TabletType_REPLICA, ServerID: 720, ExecBinlogCoordinates: BinlogCoordinates{LogFile: "mysql.000007", LogPos: 20}}
	i730 := Instance{InstanceAlias: "zone1-i730", Hostname: "i730", Port: 3306, Cell: "zone1", TabletType: topodatapb.TabletType_REPLICA, ServerID: 730, ExecBinlogCoordinates: BinlogCoordinates{LogFile: "mysql.000007", LogPos: 30}}
	instances := []*Instance{&i710, &i720, &i730}
	for _, instance := range instances {
		instance.Version = "5.6.7"
		instance.VersionComment = "MySQL"
		instance.BinlogFormat = "STATEMENT"
		instance.BinlogRowImage = "FULL"
	}
	return instances
}

func TestMkInsertSingle(t *testing.T) {
	instances := mkTestInstances()

	sql, args, err := mkInsertForInstances(nil, true, true)
	require.NoError(t, err)
	require.Equal(t, sql, "")
	require.Equal(t, len(args), 0)

	// one instance
	s1 := `INSERT OR IGNORE INTO database_instance
				(alias, hostname, port, cell, last_checked, last_attempted_check, last_check_partial_success, tablet_type, server_id, server_uuid,
				version, major_version, version_comment, binlog_server, read_only, binlog_format,
				binlog_row_image, log_bin, log_replica_updates, binary_log_file, binary_log_pos, source_host, source_port, replica_net_timeout, heartbeat_interval,
				replica_sql_running, replica_io_running, replication_sql_thread_state, replication_io_thread_state, has_replication_filters, supports_oracle_gtid, oracle_gtid, source_uuid, ancestry_uuid, executed_gtid_set, gtid_mode, gtid_purged, gtid_errant,
				source_log_file, read_source_log_pos, relay_source_log_file, exec_source_log_pos, relay_log_file, relay_log_pos, last_sql_error, last_io_error, replication_lag_seconds, replica_lag_seconds, sql_delay, replication_depth, is_co_primary, has_replication_credentials, allow_tls, semi_sync_enforced, semi_sync_primary_enabled, semi_sync_primary_timeout, semi_sync_primary_wait_for_replica_count, semi_sync_replica_enabled, semi_sync_primary_status, semi_sync_primary_clients, semi_sync_replica_status, semi_sync_blocked, last_discovery_latency, is_disk_stalled, last_seen)
		VALUES
				(?, ?, ?, ?, DATETIME('now'), DATETIME('now'), 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATETIME('now'))
       `
	a1 := `zone1-i710, i710, 3306, zone1, 1, 710, , 5.6.7, 5.6, MySQL, false, false, STATEMENT,
	FULL, false, false, , 0, , 0, 0, 0,
	false, false, 0, 0, false, false, false, , , , , , , , 0, mysql.000007, 10, , 0, , , {0 false}, {0 false}, 0, 0, false, false, false, false, false, 0, 0, false, false, 0, false, false, 0, false,`

	sql1, args1, err := mkInsertForInstances(instances[:1], false, true)
	require.NoError(t, err)
	require.Equal(t, normalizeQuery(sql1), normalizeQuery(s1))
	require.Equal(t, stripSpaces(fmtArgs(args1)), stripSpaces(a1))
}

func TestMkInsertThree(t *testing.T) {
	instances := mkTestInstances()

	// three instances
	s3 := `REPLACE INTO database_instance
				(alias, hostname, port, cell, last_checked, last_attempted_check, last_check_partial_success, tablet_type, server_id, server_uuid,
				version, major_version, version_comment, binlog_server, read_only, binlog_format,
				binlog_row_image, log_bin, log_replica_updates, binary_log_file, binary_log_pos, source_host, source_port, replica_net_timeout, heartbeat_interval,
				replica_sql_running, replica_io_running, replication_sql_thread_state, replication_io_thread_state, has_replication_filters, supports_oracle_gtid, oracle_gtid, source_uuid, ancestry_uuid, executed_gtid_set, gtid_mode, gtid_purged, gtid_errant,
				source_log_file, read_source_log_pos, relay_source_log_file, exec_source_log_pos, relay_log_file, relay_log_pos, last_sql_error, last_io_error, replication_lag_seconds, replica_lag_seconds, sql_delay, replication_depth, is_co_primary, has_replication_credentials, allow_tls, semi_sync_enforced, semi_sync_primary_enabled, semi_sync_primary_timeout, semi_sync_primary_wait_for_replica_count, semi_sync_replica_enabled, semi_sync_primary_status, semi_sync_primary_clients, semi_sync_replica_status, semi_sync_blocked, last_discovery_latency, is_disk_stalled, last_seen)
		VALUES
				(?, ?, ?, ?, DATETIME('now'), DATETIME('now'), 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATETIME('now')),
				(?, ?, ?, ?, DATETIME('now'), DATETIME('now'), 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATETIME('now')),
				(?, ?, ?, ?, DATETIME('now'), DATETIME('now'), 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATETIME('now'))
       `
	a3 := `
		zone1-i710, i710, 3306, zone1, 1, 710, , 5.6.7, 5.6, MySQL, false, false, STATEMENT, FULL, false, false, , 0, , 0, 0, 0, false, false, 0, 0, false, false, false, , , , , , , , 0, mysql.000007, 10, , 0, , , {0 false}, {0 false}, 0, 0, false, false, false, false, false, 0, 0, false, false, 0, false ,false, 0, false,
		zone1-i720, i720, 3306, zone1, 2, 720, , 5.6.7, 5.6, MySQL, false, false, STATEMENT, FULL, false, false, , 0, , 0, 0, 0, false, false, 0, 0, false, false, false, , , , , , , , 0, mysql.000007, 20, , 0, , , {0 false}, {0 false}, 0, 0, false, false, false, false, false, 0, 0, false, false, 0, false, false, 0, false,
		zone1-i730, i730, 3306, zone1, 2, 730, , 5.6.7, 5.6, MySQL, false, false, STATEMENT, FULL, false, false, , 0, , 0, 0, 0, false, false, 0, 0, false, false, false, , , , , , , , 0, mysql.000007, 30, , 0, , , {0 false}, {0 false}, 0, 0, false, false, false, false, false, 0, 0, false, false, 0, false, false, 0, false,
		`

	sql3, args3, err := mkInsertForInstances(instances[:3], true, true)
	require.NoError(t, err)
	require.Equal(t, normalizeQuery(sql3), normalizeQuery(s3))
	require.Equal(t, stripSpaces(fmtArgs(args3)), stripSpaces(a3))
}

func fmtArgs(args []any) string {
	b := &bytes.Buffer{}
	for _, a := range args {
		fmt.Fprint(b, a)
		fmt.Fprint(b, ", ")
	}
	return b.String()
}

func TestGetKeyspaceShardName(t *testing.T) {
	orcDb, err := db.OpenVTOrc()
	require.NoError(t, err)
	defer func() {
		_, err = orcDb.Exec("delete from vitess_tablet")
		require.NoError(t, err)
	}()

	ks := "ks"
	shard := "0"
	hostname := "localhost"
	var port int32 = 100
	tab100 := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone-1",
			Uid:  100,
		},
		Hostname:      hostname,
		Keyspace:      ks,
		Shard:         shard,
		Type:          topodatapb.TabletType_PRIMARY,
		MysqlHostname: hostname,
		MysqlPort:     port,
	}

	err = SaveTablet(tab100)
	require.NoError(t, err)

	keyspaceRead, shardRead, err := GetKeyspaceShardName(topoproto.TabletAliasString(tab100.Alias))
	require.NoError(t, err)
	require.Equal(t, ks, keyspaceRead)
	require.Equal(t, shard, shardRead)
}

// TestReadInstance is used to test the functionality of ReadInstance and verify its failure modes and successes.
func TestReadInstance(t *testing.T) {
	tests := []struct {
		name              string
		tabletAliasToRead string
		instanceFound     bool
	}{
		{
			name:              "Read success",
			tabletAliasToRead: "zone1-0000000100",
			instanceFound:     true,
		}, {
			name:              "Unknown tablet",
			tabletAliasToRead: "unknown-tablet",
			instanceFound:     false,
		},
	}

	// Clear the database after the test. The easiest way to do that is to run all the initialization commands again.
	defer func() {
		db.ClearVTOrcDatabase()
	}()
	for _, query := range initialSQL {
		_, err := db.ExecVTOrc(query)
		require.NoError(t, err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, found, err := ReadInstance(tt.tabletAliasToRead)
			require.NoError(t, err)
			require.Equal(t, tt.instanceFound, found)
			if tt.instanceFound {
				require.EqualValues(t, tt.tabletAliasToRead, got.InstanceAlias)
			}
		})
	}
}

// TestReadProblemInstances is used to test the functionality of ReadProblemInstances and verify its failure modes and successes.
func TestReadProblemInstances(t *testing.T) {
	// The test is intended to be used as follows. The initial data is stored into the database. Following this, some specific queries are run that each individual test specifies to get the desired state.
	tests := []struct {
		name              string
		sql               []string
		instancesRequired []string
	}{
		{
			name:              "No problems",
			sql:               nil,
			instancesRequired: nil,
		}, {
			name: "Replication stopped on a replica",
			sql: []string{
				"update database_instance set replication_sql_thread_state = 0 where alias = 'zone1-0000000112'",
			},
			instancesRequired: []string{"zone1-0000000112"},
		}, {
			name: "IO thread stopped on a replica",
			sql: []string{
				"update database_instance set replication_io_thread_state = 0 where alias = 'zone1-0000000112'",
			},
			instancesRequired: []string{"zone1-0000000112"},
		}, {
			name: "High replication lag",
			sql: []string{
				"update database_instance set replication_lag_seconds = 1000 where alias = 'zone1-0000000112'",
			},
			instancesRequired: []string{"zone1-0000000112"},
		}, {
			name: "High replication lag - replica_lag",
			sql: []string{
				"update database_instance set replica_lag_seconds = 1000 where alias = 'zone1-0000000112'",
			},
			instancesRequired: []string{"zone1-0000000112"},
		}, {
			name: "errant GTID",
			sql: []string{
				"update database_instance set gtid_errant = '729a4cc4-8680-11ed-a104-47706090afbd:1' where alias = 'zone1-0000000112'",
			},
			instancesRequired: []string{"zone1-0000000112"},
		}, {
			name: "Many failures",
			sql: []string{
				"update database_instance set gtid_errant = '729a4cc4-8680-11ed-a104-47706090afbd:1' where alias = 'zone1-0000000112'",
				"update database_instance set replication_sql_thread_state = 0 where alias = 'zone1-0000000100'",
			},
			instancesRequired: []string{"zone1-0000000112", "zone1-0000000100"},
		},
	}

	// We need to set InstancePollSeconds to a large value otherwise all the instances are reported as having problems since their last_checked is very old.
	// Setting this value to a hundred years, we ensure that this test doesn't fail with this issue for the next hundred years.
	oldVal := config.GetInstancePollTime()
	defer func() {
		config.SetInstancePollTime(oldVal)
	}()
	config.SetInstancePollTime(60 * 60 * 24 * 365 * 100 * time.Second)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Each test should clear the database. The easiest way to do that is to run all the initialization commands again
			defer func() {
				db.ClearVTOrcDatabase()
			}()

			for _, query := range append(initialSQL, tt.sql...) {
				_, err := db.ExecVTOrc(query)
				require.NoError(t, err)
			}

			instances, err := ReadProblemInstances("ks", "0")
			require.NoError(t, err)
			var tabletAliases []string
			for _, instance := range instances {
				tabletAliases = append(tabletAliases, instance.InstanceAlias)
			}
			require.ElementsMatch(t, tabletAliases, tt.instancesRequired)
		})
	}
}

// TestReadInstancesWithErrantGTIds is used to test the functionality of ReadInstancesWithErrantGTIds and verify its failure modes and successes.
func TestReadInstancesWithErrantGTIds(t *testing.T) {
	// The test is intended to be used as follows. The initial data is stored into the database. Following this, some specific queries are run that each individual test specifies to get the desired state.
	tests := []struct {
		name              string
		keyspace          string
		shard             string
		sql               []string
		instancesRequired []string
	}{
		{
			name:              "No instances with errant GTID",
			sql:               nil,
			instancesRequired: nil,
		}, {
			name: "errant GTID",
			sql: []string{
				"update database_instance set gtid_errant = '729a4cc4-8680-11ed-a104-47706090afbd:1' where alias = 'zone1-0000000112'",
			},
			instancesRequired: []string{"zone1-0000000112"},
		}, {
			name:     "keyspace filtering - success",
			keyspace: "ks",
			sql: []string{
				"update database_instance set gtid_errant = '729a4cc4-8680-11ed-a104-47706090afbd:1' where alias = 'zone1-0000000112'",
			},
			instancesRequired: []string{"zone1-0000000112"},
		}, {
			name:     "keyspace filtering - failure",
			keyspace: "unknown",
			sql: []string{
				"update database_instance set gtid_errant = '729a4cc4-8680-11ed-a104-47706090afbd:1' where alias = 'zone1-0000000112'",
			},
			instancesRequired: nil,
		}, {
			name:     "shard filtering - success",
			keyspace: "ks",
			shard:    "0",
			sql: []string{
				"update database_instance set gtid_errant = '729a4cc4-8680-11ed-a104-47706090afbd:1' where alias = 'zone1-0000000112'",
			},
			instancesRequired: []string{"zone1-0000000112"},
		}, {
			name:     "shard filtering - failure",
			keyspace: "ks",
			shard:    "unknown",
			sql: []string{
				"update database_instance set gtid_errant = '729a4cc4-8680-11ed-a104-47706090afbd:1' where alias = 'zone1-0000000112'",
			},
			instancesRequired: nil,
		},
	}

	// We need to set InstancePollSeconds to a large value otherwise all the instances are reported as having problems since their last_checked is very old.
	// Setting this value to a hundred years, we ensure that this test doesn't fail with this issue for the next hundred years.
	oldVal := config.GetInstancePollTime()
	defer func() {
		config.SetInstancePollTime(oldVal)
	}()
	config.SetInstancePollTime(60 * 60 * 24 * 365 * 100 * time.Second)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Each test should clear the database. The easiest way to do that is to run all the initialization commands again
			defer func() {
				db.ClearVTOrcDatabase()
			}()

			for _, query := range append(initialSQL, tt.sql...) {
				_, err := db.ExecVTOrc(query)
				require.NoError(t, err)
			}

			instances, err := ReadInstancesWithErrantGTIds(tt.keyspace, tt.shard)
			require.NoError(t, err)
			var tabletAliases []string
			for _, instance := range instances {
				tabletAliases = append(tabletAliases, instance.InstanceAlias)
			}
			require.ElementsMatch(t, tabletAliases, tt.instancesRequired)
		})
	}
}

// TestReadInstanceAllFields tests that we read all the fields for a specific instance.
func TestReadInstanceAllFields(t *testing.T) {
	// Clear the database after the test. The easiest way to do that is to run all the initialization commands again.
	defer func() {
		db.ClearVTOrcDatabase()
	}()
	for _, query := range initialSQL {
		_, err := db.ExecVTOrc(query)
		require.NoError(t, err)
	}

	wantInstance := &Instance{
		Hostname:                     "localhost",
		Port:                         6711,
		InstanceAlias:                "zone1-0000000100",
		TabletType:                   topodatapb.TabletType_REPLICA,
		Cell:                         "zone1",
		ServerID:                     1094500338,
		ServerUUID:                   "729a5138-8680-11ed-acf8-d6b0ef9f4eaa",
		Version:                      "8.0.31",
		VersionComment:               "Homebrew",
		FlavorName:                   "MySQL",
		ReadOnly:                     true,
		BinlogFormat:                 "ROW",
		BinlogRowImage:               "FULL",
		LogBinEnabled:                true,
		LogReplicationUpdatesEnabled: true,
		SelfBinlogCoordinates: BinlogCoordinates{
			LogFile: "vt-0000000100-bin.000001",
			LogPos:  15963,
		},
		SourceHost:                 "localhost",
		SourcePort:                 6714,
		SourceUUID:                 "729a4cc4-8680-11ed-a104-47706090afbd",
		AncestryUUID:               "729a4cc4-8680-11ed-a104-47706090afbd,729a5138-8680-11ed-acf8-d6b0ef9f4eaa",
		ReplicaNetTimeout:          8,
		HeartbeatInterval:          4,
		ReplicationSQLThreadRuning: true,
		ReplicationIOThreadRuning:  true,
		ReplicationSQLThreadState:  ReplicationThreadStateRunning,
		ReplicationIOThreadState:   ReplicationThreadStateRunning,
		HasReplicationFilters:      false,
		GTIDMode:                   "ON",
		SupportsOracleGTID:         true,
		UsingOracleGTID:            true,
		ReadBinlogCoordinates: BinlogCoordinates{
			LogFile: "vt-0000000101-bin.000001",
			LogPos:  15583,
		},
		ExecBinlogCoordinates: BinlogCoordinates{
			LogFile: "vt-0000000101-bin.000001",
			LogPos:  15583,
		},
		IsDetached: false,
		RelaylogCoordinates: BinlogCoordinates{
			LogFile: "vt-0000000100-relay-bin.000002",
			LogPos:  15815,
			Type:    RelayLog,
		},
		LastSQLError: "",
		LastIOError:  "",
		SecondsBehindPrimary: sql.NullInt64{
			Valid: true,
		},
		SQLDelay:               0,
		ExecutedGtidSet:        "729a4cc4-8680-11ed-a104-47706090afbd:1-54",
		GtidPurged:             "",
		GtidErrant:             "",
		primaryExecutedGtidSet: "",
		ReplicationLagSeconds: sql.NullInt64{
			Valid: true,
		},
		ReplicationDepth:                   1,
		IsCoPrimary:                        false,
		HasReplicationCredentials:          true,
		SemiSyncEnforced:                   false,
		SemiSyncPrimaryEnabled:             false,
		SemiSyncReplicaEnabled:             true,
		SemiSyncPrimaryTimeout:             1000000000000000000,
		SemiSyncPrimaryWaitForReplicaCount: 1,
		SemiSyncPrimaryStatus:              false,
		SemiSyncPrimaryClients:             0,
		SemiSyncReplicaStatus:              true,
		SemiSyncBlocked:                    false,
		LastSeenTimestamp:                  "2022-12-28T07:26:04Z",
		IsLastCheckValid:                   true,
		IsUpToDate:                         false,
		IsRecentlyChecked:                  false,
		SecondsSinceLastSeen:               sql.NullInt64{},
		StalledDisk:                        false,
		AllowTLS:                           false,
		Problems:                           nil,
		LastDiscoveryLatency:               0,
	}

	instance, found, err := ReadInstance(`zone1-0000000100`)
	require.NoError(t, err)
	require.True(t, found)
	instance.SecondsSinceLastSeen = sql.NullInt64{}
	instance.Problems = nil
	instance.LastDiscoveryLatency = 0
	require.EqualValues(t, wantInstance, instance)
}

// TestReadInstancesByCondition is used to test the functionality of readInstancesByCondition and verify its failure modes and successes.
func TestReadInstancesByCondition(t *testing.T) {
	tests := []struct {
		name              string
		condition         string
		args              []any
		sort              string
		instancesRequired []string
	}{
		{
			name:              "All instances with no sort",
			condition:         "1=1",
			instancesRequired: []string{"zone1-0000000100", "zone1-0000000101", "zone1-0000000112", "zone2-0000000200"},
		}, {
			name:              "Filtering by replication_depth",
			condition:         "replication_depth=1",
			instancesRequired: []string{"zone1-0000000100", "zone1-0000000112", "zone2-0000000200"},
		}, {
			name:              "Filtering by exact alias",
			condition:         "alias='zone1-0000000100'",
			instancesRequired: []string{"zone1-0000000100"},
		}, {
			name:      "No qualifying tablets",
			condition: "replication_depth=15",
		}, {
			name:              "Replica net timeout being 8",
			condition:         "replica_net_timeout=8",
			sort:              "alias asc",
			instancesRequired: []string{"zone1-0000000100", "zone1-0000000112", "zone2-0000000200"},
		}, {
			name:              "heartbeat interval being 4",
			condition:         "heartbeat_interval=4.0",
			sort:              "alias asc",
			instancesRequired: []string{"zone1-0000000100", "zone1-0000000112", "zone2-0000000200"},
		}, {
			name:              "tablet type is primary",
			condition:         "database_instance.tablet_type=1",
			sort:              "alias asc",
			instancesRequired: []string{"zone1-0000000101"},
		},
	}

	// Clear the database after the test. The easiest way to do that is to run all the initialization commands again.
	defer func() {
		db.ClearVTOrcDatabase()
	}()
	for _, query := range initialSQL {
		_, err := db.ExecVTOrc(query)
		require.NoError(t, err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instances, err := readInstancesByCondition(tt.condition, tt.args, tt.sort)
			require.NoError(t, err)
			var tabletAliases []string
			for _, instance := range instances {
				tabletAliases = append(tabletAliases, instance.InstanceAlias)
			}
			require.EqualValues(t, tt.instancesRequired, tabletAliases)
		})
	}
}

// TestReadOutdatedInstanceKeys is used to test the functionality of ReadOutdatedInstanceKeys and verify its failure modes and successes.
func TestReadOutdatedInstanceKeys(t *testing.T) {
	// The test is intended to be used as follows. The initial data is stored into the database. Following this, some specific queries are run that each individual test specifies to get the desired state.
	tests := []struct {
		name              string
		sql               []string
		instancesRequired []string
	}{
		{
			name:              "No problems",
			sql:               []string{"update database_instance set last_checked = DATETIME('now')"},
			instancesRequired: nil,
		}, {
			name: "One instance is outdated",
			sql: []string{
				"update database_instance set last_checked = DATETIME('now')",
				"update database_instance set last_checked = DATETIME('now', '-1 hour') where alias = 'zone1-0000000100'",
			},
			instancesRequired: []string{"zone1-0000000100"},
		}, {
			name: "One instance doesn't have myql data",
			sql: []string{
				"update database_instance set last_checked = DATETIME('now')",
				`INSERT INTO vitess_tablet VALUES('zone1-0000000103','localhost',7706,'ks','0','zone1',2,'0001-01-01 00:00:00+00:00','');`,
			},
			instancesRequired: []string{"zone1-0000000103"},
		}, {
			name: "One instance doesn't have myql data and one is outdated",
			sql: []string{
				"update database_instance set last_checked = DATETIME('now')",
				"update database_instance set last_checked = DATETIME('now', '-1 hour') where alias = 'zone1-0000000100'",
				`INSERT INTO vitess_tablet VALUES('zone1-0000000103','localhost',7706,'ks','0','zone1',2,'0001-01-01 00:00:00+00:00','');`,
			},
			instancesRequired: []string{"zone1-0000000103", "zone1-0000000100"},
		},
	}

	// wait for the forgetAliases cache to be initialized to prevent data race.
	waitForCacheInitialization()

	// We are setting InstancePollSeconds to 59 minutes, just for the test.
	oldVal := config.GetInstancePollTime()
	oldCache := forgetAliases
	defer func() {
		forgetAliases = oldCache
		config.SetInstancePollTime(oldVal)
	}()
	config.SetInstancePollTime(60 * 25 * time.Second)
	forgetAliases = cache.New(time.Minute, time.Minute)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Each test should clear the database. The easiest way to do that is to run all the initialization commands again
			defer func() {
				db.ClearVTOrcDatabase()
			}()

			for _, query := range append(initialSQL, tt.sql...) {
				_, err := db.ExecVTOrc(query)
				require.NoError(t, err)
			}

			tabletAliases, err := ReadOutdatedInstanceKeys()

			errInDataCollection := db.QueryVTOrcRowsMap(`select alias,
last_checked,
last_attempted_check,
ROUND((JULIANDAY(DATETIME('now')) - JULIANDAY(last_checked)) * 86400) AS difference,
last_attempted_check <= last_checked as use1,
last_checked < DATETIME('now', '-1500 second') as is_outdated1,
last_checked < DATETIME('now', '-3000 second') as is_outdated2
from database_instance`, func(rowMap sqlutils.RowMap) error {
				log.Errorf("Row in database_instance - %+v", rowMap)
				return nil
			})
			require.NoError(t, errInDataCollection)
			require.NoError(t, err)
			require.ElementsMatch(t, tabletAliases, tt.instancesRequired)
		})
	}
}

// TestUpdateInstanceLastChecked is used to test the functionality of UpdateInstanceLastChecked and verify its failure modes and successes.
func TestUpdateInstanceLastChecked(t *testing.T) {
	tests := []struct {
		name             string
		tabletAlias      string
		partialSuccess   bool
		stalledDisk      bool
		conditionToCheck string
	}{
		{
			name:             "Verify updated last checked",
			tabletAlias:      "zone1-0000000100",
			partialSuccess:   false,
			stalledDisk:      false,
			conditionToCheck: "last_checked >= DATETIME('now', '-30 second') and last_check_partial_success = false and is_disk_stalled = false",
		}, {
			name:             "Verify partial success",
			tabletAlias:      "zone1-0000000100",
			partialSuccess:   true,
			stalledDisk:      false,
			conditionToCheck: "last_checked >= datetime('now', '-30 second') and last_check_partial_success = true and is_disk_stalled = false",
		}, {
			name:             "Verify stalled disk",
			tabletAlias:      "zone1-0000000100",
			partialSuccess:   false,
			stalledDisk:      true,
			conditionToCheck: "last_checked >= DATETIME('now', '-30 second') and last_check_partial_success = false and is_disk_stalled = true",
		}, {
			name:           "Verify no error on unknown tablet",
			tabletAlias:    "unknown tablet",
			partialSuccess: true,
			stalledDisk:    true,
		},
	}

	// Clear the database after the test. The easiest way to do that is to run all the initialization commands again.
	defer func() {
		db.ClearVTOrcDatabase()
	}()
	for _, query := range initialSQL {
		_, err := db.ExecVTOrc(query)
		require.NoError(t, err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := UpdateInstanceLastChecked(tt.tabletAlias, tt.partialSuccess, tt.stalledDisk)
			require.NoError(t, err)

			if tt.conditionToCheck != "" {
				// Verify the instance we just updated satisfies the condition specified.
				instances, err := readInstancesByCondition(tt.conditionToCheck, nil, "")
				require.NoError(t, err)
				var tabletAliases []string
				for _, instance := range instances {
					tabletAliases = append(tabletAliases, instance.InstanceAlias)
				}
				require.Contains(t, tabletAliases, tt.tabletAlias)
			}
		})
	}
}

// UpdateInstanceLastAttemptedCheck is used to test the functionality of UpdateInstanceLastAttemptedCheck and verify its failure modes and successes.
func TestUpdateInstanceLastAttemptedCheck(t *testing.T) {
	tests := []struct {
		name             string
		tabletAlias      string
		conditionToCheck string
	}{
		{
			name:             "Verify updated last checked",
			tabletAlias:      "zone1-0000000100",
			conditionToCheck: "last_attempted_check >= DATETIME('now', '-30 second')",
		}, {
			name:        "Verify no error on unknown tablet",
			tabletAlias: "unknown tablet",
		},
	}

	// Clear the database after the test. The easiest way to do that is to run all the initialization commands again.
	defer func() {
		db.ClearVTOrcDatabase()
	}()
	for _, query := range initialSQL {
		_, err := db.ExecVTOrc(query)
		require.NoError(t, err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := UpdateInstanceLastAttemptedCheck(tt.tabletAlias)
			require.NoError(t, err)

			if tt.conditionToCheck != "" {
				// Verify the instance we just updated satisfies the condition specified.
				instances, err := readInstancesByCondition(tt.conditionToCheck, nil, "")
				require.NoError(t, err)
				var tabletAliases []string
				for _, instance := range instances {
					tabletAliases = append(tabletAliases, instance.InstanceAlias)
				}
				require.Contains(t, tabletAliases, tt.tabletAlias)
			}
		})
	}
}

// TestForgetInstanceAndInstanceIsForgotten tests the functionality of ForgetInstance and InstanceIsForgotten together.
func TestForgetInstanceAndInstanceIsForgotten(t *testing.T) {
	tests := []struct {
		name              string
		tabletAlias       string
		errExpected       string
		instanceForgotten bool
		tabletsExpected   []string
	}{
		{
			name:              "Unknown tablet",
			tabletAlias:       "unknown-tablet",
			errExpected:       "ForgetInstance(): tablet unknown-tablet not found",
			instanceForgotten: true,
			tabletsExpected:   []string{"zone1-0000000100", "zone1-0000000101", "zone1-0000000112", "zone2-0000000200"},
		}, {
			name:              "Empty tabletAlias",
			tabletAlias:       "",
			errExpected:       "ForgetInstance(): empty tabletAlias",
			instanceForgotten: false,
			tabletsExpected:   []string{"zone1-0000000100", "zone1-0000000101", "zone1-0000000112", "zone2-0000000200"},
		}, {
			name:              "Success",
			tabletAlias:       "zone1-0000000112",
			instanceForgotten: true,
			tabletsExpected:   []string{"zone1-0000000100", "zone1-0000000101", "zone2-0000000200"},
		},
	}

	// wait for the forgetAliases cache to be initialized to prevent data race.
	waitForCacheInitialization()

	oldCache := forgetAliases
	// Clear the database after the test. The easiest way to do that is to run all the initialization commands again.
	defer func() {
		forgetAliases = oldCache
		db.ClearVTOrcDatabase()
	}()
	forgetAliases = cache.New(time.Minute, time.Minute)

	for _, query := range initialSQL {
		_, err := db.ExecVTOrc(query)
		require.NoError(t, err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ForgetInstance(tt.tabletAlias)
			if tt.errExpected != "" {
				require.EqualError(t, err, tt.errExpected)
			} else {
				require.NoError(t, err)
			}
			isForgotten := InstanceIsForgotten(tt.tabletAlias)
			require.Equal(t, tt.instanceForgotten, isForgotten)

			instances, err := readInstancesByCondition("1=1", nil, "")
			require.NoError(t, err)
			var tabletAliases []string
			for _, instance := range instances {
				tabletAliases = append(tabletAliases, instance.InstanceAlias)
			}
			require.EqualValues(t, tt.tabletsExpected, tabletAliases)
		})
	}
}

func TestSnapshotTopologies(t *testing.T) {
	// Clear the database after the test. The easiest way to do that is to run all the initialization commands again.
	defer func() {
		db.ClearVTOrcDatabase()
	}()

	for _, query := range initialSQL {
		_, err := db.ExecVTOrc(query)
		require.NoError(t, err)
	}

	err := SnapshotTopologies()
	require.NoError(t, err)

	query := "select alias from database_instance_topology_history"
	var tabletAliases []string
	err = db.QueryVTOrc(query, nil, func(rowMap sqlutils.RowMap) error {
		tabletAliases = append(tabletAliases, rowMap.GetString("alias"))
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, []string{"zone1-0000000100", "zone1-0000000101", "zone1-0000000112", "zone2-0000000200"}, tabletAliases)
}

// waitForCacheInitialization waits for the cache to be initialized to prevent data race in tests
// that alter the cache or depend on its behaviour.
func waitForCacheInitialization() {
	for {
		if cacheInitializationCompleted.Load() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestGetDatabaseState(t *testing.T) {
	// Clear the database after the test. The easiest way to do that is to run all the initialization commands again.
	defer func() {
		db.ClearVTOrcDatabase()
	}()

	for _, query := range initialSQL {
		_, err := db.ExecVTOrc(query)
		require.NoError(t, err)
	}

	ds, err := GetDatabaseState()
	require.NoError(t, err)
	require.Contains(t, ds, `"alias": "zone1-0000000112"`)
}

func TestExpireTableData(t *testing.T) {
	oldVal := config.GetAuditPurgeDays()
	config.SetAuditPurgeDays(10)
	defer func() {
		config.SetAuditPurgeDays(oldVal)
	}()

	tests := []struct {
		name             string
		tableName        string
		insertQuery      string
		timestampColumn  string
		expectedRowCount int
	}{
		{
			name:             "ExpireAudit",
			tableName:        "audit",
			timestampColumn:  "audit_timestamp",
			expectedRowCount: 1,
			insertQuery: `INSERT INTO audit (audit_id, audit_timestamp, audit_type, alias, message, keyspace, shard) VALUES
(1, DATETIME('now', '-50 DAY'), 'a','a','a','a','a'),
(2, DATETIME('now', '-5 DAY'), 'a','a','a','a','a')`,
		},
		{
			name:             "ExpireRecoveryDetectionHistory",
			tableName:        "recovery_detection",
			timestampColumn:  "detection_timestamp",
			expectedRowCount: 2,
			insertQuery: `INSERT INTO recovery_detection (detection_id, detection_timestamp, alias, analysis, keyspace, shard) VALUES
(1, DATETIME('now', '-3 DAY'),'a','a','a','a'),
(2, DATETIME('now', '-5 DAY'),'a','a','a','a'),
(3, DATETIME('now', '-15 DAY'),'a','a','a','a')`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear the database after the test. The easiest way to do that is to run all the initialization commands again.
			defer func() {
				db.ClearVTOrcDatabase()
			}()
			_, err := db.ExecVTOrc(tt.insertQuery)
			require.NoError(t, err)

			err = ExpireTableData(tt.tableName, tt.timestampColumn)
			require.NoError(t, err)

			rowsCount := 0
			err = db.QueryVTOrc(`select * from `+tt.tableName, nil, func(rowMap sqlutils.RowMap) error {
				rowsCount++
				return nil
			})
			require.NoError(t, err)
			require.EqualValues(t, tt.expectedRowCount, rowsCount)
		})
	}
}

func TestDetectErrantGTIDs(t *testing.T) {
	tests := []struct {
		name            string
		instance        *Instance
		primaryInstance *Instance
		wantErr         bool
		wantErrantGTID  string
	}{
		{
			name: "No errant GTIDs",
			instance: &Instance{
				ExecutedGtidSet:        "230ea8ea-81e3-11e4-972a-e25ec4bd140a:1-10539,8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-34",
				primaryExecutedGtidSet: "230ea8ea-81e3-11e4-972a-e25ec4bd140a:1-10591,8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-34",
				AncestryUUID:           "316d193c-70e5-11e5-adb2-ecf4bb2262ff,230ea8ea-81e3-11e4-972a-e25ec4bd140a",
				ServerUUID:             "316d193c-70e5-11e5-adb2-ecf4bb2262ff",
				SourceUUID:             "230ea8ea-81e3-11e4-972a-e25ec4bd140a",
			},
		}, {
			name: "Errant GTIDs on replica",
			instance: &Instance{
				ExecutedGtidSet:        "230ea8ea-81e3-11e4-972a-e25ec4bd140a:1-10539,8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-34,316d193c-70e5-11e5-adb2-ecf4bb2262ff:34",
				primaryExecutedGtidSet: "230ea8ea-81e3-11e4-972a-e25ec4bd140a:1-10591,8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-34",
				AncestryUUID:           "316d193c-70e5-11e5-adb2-ecf4bb2262ff,230ea8ea-81e3-11e4-972a-e25ec4bd140a",
				ServerUUID:             "316d193c-70e5-11e5-adb2-ecf4bb2262ff",
				SourceUUID:             "230ea8ea-81e3-11e4-972a-e25ec4bd140a",
			},
			wantErrantGTID: "316d193c-70e5-11e5-adb2-ecf4bb2262ff:34",
		},
		{
			name: "No errant GTIDs on old primary",
			instance: &Instance{
				ExecutedGtidSet: "230ea8ea-81e3-11e4-972a-e25ec4bd140a:1-10539,8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-34,316d193c-70e5-11e5-adb2-ecf4bb2262ff:1-341",
				AncestryUUID:    "316d193c-70e5-11e5-adb2-ecf4bb2262ff",
				ServerUUID:      "316d193c-70e5-11e5-adb2-ecf4bb2262ff",
			},
			primaryInstance: &Instance{
				SourceHost:      "",
				ExecutedGtidSet: "230ea8ea-81e3-11e4-972a-e25ec4bd140a:1-10589,8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-34,316d193c-70e5-11e5-adb2-ecf4bb2262ff:1-341",
			},
		},
		{
			name: "Errant GTIDs on old primary",
			instance: &Instance{
				ExecutedGtidSet: "230ea8ea-81e3-11e4-972a-e25ec4bd140a:1-10539,8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-34,316d193c-70e5-11e5-adb2-ecf4bb2262ff:1-342",
				AncestryUUID:    "316d193c-70e5-11e5-adb2-ecf4bb2262ff",
				ServerUUID:      "316d193c-70e5-11e5-adb2-ecf4bb2262ff",
			},
			primaryInstance: &Instance{
				SourceHost:      "",
				ExecutedGtidSet: "230ea8ea-81e3-11e4-972a-e25ec4bd140a:1-10589,8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-34,316d193c-70e5-11e5-adb2-ecf4bb2262ff:1-341",
			},
			wantErrantGTID: "316d193c-70e5-11e5-adb2-ecf4bb2262ff:342",
		}, {
			name: "Old information for new primary",
			instance: &Instance{
				ExecutedGtidSet: "230ea8ea-81e3-11e4-972a-e25ec4bd140a:1-10539,8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-34,316d193c-70e5-11e5-adb2-ecf4bb2262ff:1-342",
				AncestryUUID:    "316d193c-70e5-11e5-adb2-ecf4bb2262ff",
				ServerUUID:      "316d193c-70e5-11e5-adb2-ecf4bb2262ff",
			},
			primaryInstance: &Instance{
				SourceHost:      "localhost",
				ExecutedGtidSet: "230ea8ea-81e3-11e4-972a-e25ec4bd140a:1-10539,8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-34,316d193c-70e5-11e5-adb2-ecf4bb2262ff:1-311",
			},
		},
	}

	keyspaceName := "ks"
	shardName := "0"
	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone-1",
			Uid:  100,
		},
		Keyspace: keyspaceName,
		Shard:    shardName,
	}
	primaryTablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone-1",
			Uid:  101,
		},
		Keyspace: keyspaceName,
		Shard:    shardName,
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear the database after the test. The easiest way to do that is to run all the initialization commands again.
			defer func() {
				db.ClearVTOrcDatabase()
			}()
			db.ClearVTOrcDatabase()

			// Save shard record for the primary tablet.
			err := SaveShard(topo.NewShardInfo(keyspaceName, shardName, &topodatapb.Shard{
				PrimaryAlias: primaryTablet.Alias,
			}, nil))
			require.NoError(t, err)

			if tt.primaryInstance != nil {
				tt.primaryInstance.InstanceAlias = topoproto.TabletAliasString(primaryTablet.Alias)
				err = SaveTablet(primaryTablet)
				require.NoError(t, err)
				err = WriteInstance(tt.primaryInstance, true, nil)
				require.NoError(t, err)
			}

			tt.instance.InstanceAlias = topoproto.TabletAliasString(tablet.Alias)
			err = detectErrantGTIDs(tt.instance, tablet)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.EqualValues(t, tt.wantErrantGTID, tt.instance.GtidErrant)
		})
	}
}

// TestPrimaryErrantGTIDs tests that we don't run Errant GTID detection on the primary tablet itself!
func TestPrimaryErrantGTIDs(t *testing.T) {
	// Clear the database after the test. The easiest way to do that is to run all the initialization commands again.
	defer func() {
		db.ClearVTOrcDatabase()
	}()
	db.ClearVTOrcDatabase()
	keyspaceName := "ks"
	shardName := "0"
	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone-1",
			Uid:  100,
		},
		Keyspace: keyspaceName,
		Shard:    shardName,
	}
	instance := &Instance{
		SourceHost:      "",
		ExecutedGtidSet: "230ea8ea-81e3-11e4-972a-e25ec4bd140a:1-10589,8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-34,316d193c-70e5-11e5-adb2-ecf4bb2262ff:1-341",
		InstanceAlias:   topoproto.TabletAliasString(tablet.Alias),
	}

	// Save shard record for the primary tablet.
	err := SaveShard(topo.NewShardInfo(keyspaceName, shardName, &topodatapb.Shard{
		PrimaryAlias: tablet.Alias,
	}, nil))
	require.NoError(t, err)

	// Store the tablet record and the instance.
	err = SaveTablet(tablet)
	require.NoError(t, err)
	err = WriteInstance(instance, true, nil)
	require.NoError(t, err)

	// After this if we read a new information for the record that updates its
	// gtid set further, we shouldn't be detecting errant GTIDs on it since it is the primary!
	// We shouldn't be comparing it with a previous version of itself!
	instance.ExecutedGtidSet = "230ea8ea-81e3-11e4-972a-e25ec4bd140a:1-10589,8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-34,316d193c-70e5-11e5-adb2-ecf4bb2262ff:1-351"
	err = detectErrantGTIDs(instance, tablet)
	require.NoError(t, err)
	require.EqualValues(t, "", instance.GtidErrant)
}

package inst

import (
	"bytes"
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
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
)

var (
	spacesRegexp = regexp.MustCompile(`[ \t\n\r]+`)
)

func normalizeQuery(name string) string {
	name = strings.Replace(name, "`", "", -1)
	name = spacesRegexp.ReplaceAllString(name, " ")
	name = strings.TrimSpace(name)
	return name
}

func stripSpaces(s string) string {
	s = spacesRegexp.ReplaceAllString(s, "")
	return s
}

func mkTestInstances() []*Instance {
	i710 := Instance{InstanceAlias: "zone1-i710", Hostname: "i710", Port: 3306, ServerID: 710, ExecBinlogCoordinates: BinlogCoordinates{LogFile: "mysql.000007", LogPos: 10}}
	i720 := Instance{InstanceAlias: "zone1-i720", Hostname: "i720", Port: 3306, ServerID: 720, ExecBinlogCoordinates: BinlogCoordinates{LogFile: "mysql.000007", LogPos: 20}}
	i730 := Instance{InstanceAlias: "zone1-i730", Hostname: "i730", Port: 3306, ServerID: 730, ExecBinlogCoordinates: BinlogCoordinates{LogFile: "mysql.000007", LogPos: 30}}
	instances := []*Instance{&i710, &i720, &i730}
	for _, instance := range instances {
		instance.Version = "5.6.7"
		instance.VersionComment = "MySQL"
		instance.BinlogFormat = "STATEMENT"
		instance.BinlogRowImage = "FULL"
	}
	return instances
}

func TestMkInsertOdkuSingle(t *testing.T) {
	instances := mkTestInstances()

	sql, args, err := mkInsertOdkuForInstances(nil, true, true)
	require.NoError(t, err)
	require.Equal(t, sql, "")
	require.Equal(t, len(args), 0)

	// one instance
	s1 := `INSERT ignore INTO database_instance
				(alias, hostname, port, last_checked, last_attempted_check, last_check_partial_success, server_id, server_uuid,
				version, major_version, version_comment, binlog_server, read_only, binlog_format,
				binlog_row_image, log_bin, log_replica_updates, binary_log_file, binary_log_pos, source_host, source_port,
				replica_sql_running, replica_io_running, replication_sql_thread_state, replication_io_thread_state, has_replication_filters, supports_oracle_gtid, oracle_gtid, source_uuid, ancestry_uuid, executed_gtid_set, gtid_mode, gtid_purged, gtid_errant, mariadb_gtid, pseudo_gtid,
				source_log_file, read_source_log_pos, relay_source_log_file, exec_source_log_pos, relay_log_file, relay_log_pos, last_sql_error, last_io_error, replication_lag_seconds, replica_lag_seconds, sql_delay, data_center, region, physical_environment, replication_depth, is_co_primary, has_replication_credentials, allow_tls, semi_sync_enforced, semi_sync_primary_enabled, semi_sync_primary_timeout, semi_sync_primary_wait_for_replica_count, semi_sync_replica_enabled, semi_sync_primary_status, semi_sync_primary_clients, semi_sync_replica_status, last_discovery_latency, last_seen)
		VALUES
				(?, ?, ?, NOW(), NOW(), 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
		ON DUPLICATE KEY UPDATE
				alias=VALUES(alias), hostname=VALUES(hostname), port=VALUES(port), last_checked=VALUES(last_checked), last_attempted_check=VALUES(last_attempted_check), last_check_partial_success=VALUES(last_check_partial_success), server_id=VALUES(server_id), server_uuid=VALUES(server_uuid), version=VALUES(version), major_version=VALUES(major_version), version_comment=VALUES(version_comment), binlog_server=VALUES(binlog_server), read_only=VALUES(read_only), binlog_format=VALUES(binlog_format), binlog_row_image=VALUES(binlog_row_image), log_bin=VALUES(log_bin), log_replica_updates=VALUES(log_replica_updates), binary_log_file=VALUES(binary_log_file), binary_log_pos=VALUES(binary_log_pos), source_host=VALUES(source_host), source_port=VALUES(source_port), replica_sql_running=VALUES(replica_sql_running), replica_io_running=VALUES(replica_io_running), replication_sql_thread_state=VALUES(replication_sql_thread_state), replication_io_thread_state=VALUES(replication_io_thread_state), has_replication_filters=VALUES(has_replication_filters), supports_oracle_gtid=VALUES(supports_oracle_gtid), oracle_gtid=VALUES(oracle_gtid), source_uuid=VALUES(source_uuid), ancestry_uuid=VALUES(ancestry_uuid), executed_gtid_set=VALUES(executed_gtid_set), gtid_mode=VALUES(gtid_mode), gtid_purged=VALUES(gtid_purged), gtid_errant=VALUES(gtid_errant), mariadb_gtid=VALUES(mariadb_gtid), pseudo_gtid=VALUES(pseudo_gtid), source_log_file=VALUES(source_log_file), read_source_log_pos=VALUES(read_source_log_pos), relay_source_log_file=VALUES(relay_source_log_file), exec_source_log_pos=VALUES(exec_source_log_pos), relay_log_file=VALUES(relay_log_file), relay_log_pos=VALUES(relay_log_pos), last_sql_error=VALUES(last_sql_error), last_io_error=VALUES(last_io_error), replication_lag_seconds=VALUES(replication_lag_seconds), replica_lag_seconds=VALUES(replica_lag_seconds), sql_delay=VALUES(sql_delay), data_center=VALUES(data_center), region=VALUES(region), physical_environment=VALUES(physical_environment), replication_depth=VALUES(replication_depth), is_co_primary=VALUES(is_co_primary), has_replication_credentials=VALUES(has_replication_credentials), allow_tls=VALUES(allow_tls),
				semi_sync_enforced=VALUES(semi_sync_enforced), semi_sync_primary_enabled=VALUES(semi_sync_primary_enabled), semi_sync_primary_timeout=VALUES(semi_sync_primary_timeout), semi_sync_primary_wait_for_replica_count=VALUES(semi_sync_primary_wait_for_replica_count), semi_sync_replica_enabled=VALUES(semi_sync_replica_enabled), semi_sync_primary_status=VALUES(semi_sync_primary_status), semi_sync_primary_clients=VALUES(semi_sync_primary_clients), semi_sync_replica_status=VALUES(semi_sync_replica_status),
				last_discovery_latency=VALUES(last_discovery_latency), last_seen=VALUES(last_seen)
       `
	a1 := `zone1-i710, i710, 3306, 710, , 5.6.7, 5.6, MySQL, false, false, STATEMENT,
	FULL, false, false, , 0, , 0,
	false, false, 0, 0, false, false, false, , , , , , , false, false, , 0, mysql.000007, 10, , 0, , , {0 false}, {0 false}, 0, , , , 0, false, false, false, false, false, 0, 0, false, false, 0, false, 0,`

	sql1, args1, err := mkInsertOdkuForInstances(instances[:1], false, true)
	require.NoError(t, err)
	require.Equal(t, normalizeQuery(sql1), normalizeQuery(s1))
	require.Equal(t, stripSpaces(fmtArgs(args1)), stripSpaces(a1))
}

func TestMkInsertOdkuThree(t *testing.T) {
	instances := mkTestInstances()

	// three instances
	s3 := `INSERT  INTO database_instance
				(alias, hostname, port, last_checked, last_attempted_check, last_check_partial_success, server_id, server_uuid,
				version, major_version, version_comment, binlog_server, read_only, binlog_format,
				binlog_row_image, log_bin, log_replica_updates, binary_log_file, binary_log_pos, source_host, source_port,
				replica_sql_running, replica_io_running, replication_sql_thread_state, replication_io_thread_state, has_replication_filters, supports_oracle_gtid, oracle_gtid, source_uuid, ancestry_uuid, executed_gtid_set, gtid_mode, gtid_purged, gtid_errant, mariadb_gtid, pseudo_gtid,
				source_log_file, read_source_log_pos, relay_source_log_file, exec_source_log_pos, relay_log_file, relay_log_pos, last_sql_error, last_io_error, replication_lag_seconds, replica_lag_seconds, sql_delay, data_center, region, physical_environment, replication_depth, is_co_primary, has_replication_credentials, allow_tls, semi_sync_enforced, semi_sync_primary_enabled, semi_sync_primary_timeout, semi_sync_primary_wait_for_replica_count, semi_sync_replica_enabled, semi_sync_primary_status, semi_sync_primary_clients, semi_sync_replica_status, last_discovery_latency, last_seen)
		VALUES
				(?, ?, ?, NOW(), NOW(), 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW()),
				(?, ?, ?, NOW(), NOW(), 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW()),
				(?, ?, ?, NOW(), NOW(), 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
		ON DUPLICATE KEY UPDATE
				alias=VALUES(alias), hostname=VALUES(hostname), port=VALUES(port), last_checked=VALUES(last_checked), last_attempted_check=VALUES(last_attempted_check), last_check_partial_success=VALUES(last_check_partial_success), server_id=VALUES(server_id), server_uuid=VALUES(server_uuid), version=VALUES(version), major_version=VALUES(major_version), version_comment=VALUES(version_comment), binlog_server=VALUES(binlog_server), read_only=VALUES(read_only), binlog_format=VALUES(binlog_format), binlog_row_image=VALUES(binlog_row_image), log_bin=VALUES(log_bin), log_replica_updates=VALUES(log_replica_updates), binary_log_file=VALUES(binary_log_file), binary_log_pos=VALUES(binary_log_pos), source_host=VALUES(source_host), source_port=VALUES(source_port), replica_sql_running=VALUES(replica_sql_running), replica_io_running=VALUES(replica_io_running), replication_sql_thread_state=VALUES(replication_sql_thread_state), replication_io_thread_state=VALUES(replication_io_thread_state), has_replication_filters=VALUES(has_replication_filters), supports_oracle_gtid=VALUES(supports_oracle_gtid), oracle_gtid=VALUES(oracle_gtid), source_uuid=VALUES(source_uuid), ancestry_uuid=VALUES(ancestry_uuid), executed_gtid_set=VALUES(executed_gtid_set), gtid_mode=VALUES(gtid_mode), gtid_purged=VALUES(gtid_purged), gtid_errant=VALUES(gtid_errant), mariadb_gtid=VALUES(mariadb_gtid), pseudo_gtid=VALUES(pseudo_gtid), source_log_file=VALUES(source_log_file), read_source_log_pos=VALUES(read_source_log_pos), relay_source_log_file=VALUES(relay_source_log_file), exec_source_log_pos=VALUES(exec_source_log_pos), relay_log_file=VALUES(relay_log_file), relay_log_pos=VALUES(relay_log_pos), last_sql_error=VALUES(last_sql_error), last_io_error=VALUES(last_io_error), replication_lag_seconds=VALUES(replication_lag_seconds), replica_lag_seconds=VALUES(replica_lag_seconds), sql_delay=VALUES(sql_delay), data_center=VALUES(data_center), region=VALUES(region),
				physical_environment=VALUES(physical_environment), replication_depth=VALUES(replication_depth), is_co_primary=VALUES(is_co_primary), has_replication_credentials=VALUES(has_replication_credentials), allow_tls=VALUES(allow_tls), semi_sync_enforced=VALUES(semi_sync_enforced),
				semi_sync_primary_enabled=VALUES(semi_sync_primary_enabled), semi_sync_primary_timeout=VALUES(semi_sync_primary_timeout), semi_sync_primary_wait_for_replica_count=VALUES(semi_sync_primary_wait_for_replica_count), semi_sync_replica_enabled=VALUES(semi_sync_replica_enabled), semi_sync_primary_status=VALUES(semi_sync_primary_status), semi_sync_primary_clients=VALUES(semi_sync_primary_clients), semi_sync_replica_status=VALUES(semi_sync_replica_status),
				last_discovery_latency=VALUES(last_discovery_latency), last_seen=VALUES(last_seen)
       `
	a3 := `
		zone1-i710, i710, 3306, 710, , 5.6.7, 5.6, MySQL, false, false, STATEMENT, FULL, false, false, , 0, , 0, false, false, 0, 0, false, false, false, , , , , , , false, false, , 0, mysql.000007, 10, , 0, , , {0 false}, {0 false}, 0, , , , 0, false, false, false, false, false, 0, 0, false, false, 0, false, 0,
		zone1-i720, i720, 3306, 720, , 5.6.7, 5.6, MySQL, false, false, STATEMENT, FULL, false, false, , 0, , 0, false, false, 0, 0, false, false, false, , , , , , , false, false, , 0, mysql.000007, 20, , 0, , , {0 false}, {0 false}, 0, , , , 0, false, false, false, false, false, 0, 0, false, false, 0, false, 0,
		zone1-i730, i730, 3306, 730, , 5.6.7, 5.6, MySQL, false, false, STATEMENT, FULL, false, false, , 0, , 0, false, false, 0, 0, false, false, false, , , , , , , false, false, , 0, mysql.000007, 30, , 0, , , {0 false}, {0 false}, 0, , , , 0, false, false, false, false, false, 0, 0, false, false, 0, false, 0,
		`

	sql3, args3, err := mkInsertOdkuForInstances(instances[:3], true, true)
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

// TestReadReplicaInstances is used to test the functionality of ReadReplicaInstances and verify its failure modes and successes.
func TestReadReplicaInstances(t *testing.T) {
	tests := []struct {
		name        string
		tabletPort  int
		replicasLen int
	}{
		{
			name: "Read success - Multiple replicas",
			// This tabletPort corresponds to zone1-0000000101. That is the primary for the data inserted.
			// Check initialSQL for more details.
			tabletPort:  6714,
			replicasLen: 3,
		}, {
			name: "Unknown tablet",
			// This tabletPort corresponds to none of the tablets.
			// Check initialSQL for more details.
			tabletPort:  343,
			replicasLen: 0,
		}, {
			name: "Read success - No replicas",
			// This tabletPort corresponds to zone1-0000000100. That is a replica tablet, with no replicas of its own.
			// Check initialSQL for more details.
			tabletPort:  6711,
			replicasLen: 0,
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

			instances, err := ReadReplicaInstances("localhost", tt.tabletPort)
			require.NoError(t, err)
			require.EqualValues(t, tt.replicasLen, len(instances))
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
	oldVal := config.Config.InstancePollSeconds
	defer func() {
		config.Config.InstancePollSeconds = oldVal
	}()
	config.Config.InstancePollSeconds = 60 * 60 * 24 * 365 * 100

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
			name:              "All instances sort by data_center descending and then alias ascending",
			condition:         "1=1",
			sort:              "data_center desc, alias asc",
			instancesRequired: []string{"zone2-0000000200", "zone1-0000000100", "zone1-0000000101", "zone1-0000000112"},
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
			sql:               []string{"update database_instance set last_checked = now()"},
			instancesRequired: nil,
		}, {
			name: "One instance is outdated",
			sql: []string{
				"update database_instance set last_checked = now()",
				"update database_instance set last_checked = datetime(now(), '-1 hour') where alias = 'zone1-0000000100'",
			},
			instancesRequired: []string{"zone1-0000000100"},
		}, {
			name: "One instance doesn't have myql data",
			sql: []string{
				"update database_instance set last_checked = now()",
				`INSERT INTO vitess_tablet VALUES('zone1-0000000103','localhost',7706,'ks','0','zone1',2,'0001-01-01 00:00:00+00:00','');`,
			},
			instancesRequired: []string{"zone1-0000000103"},
		}, {
			name: "One instance doesn't have myql data and one is outdated",
			sql: []string{
				"update database_instance set last_checked = now()",
				"update database_instance set last_checked = datetime(now(), '-1 hour') where alias = 'zone1-0000000100'",
				`INSERT INTO vitess_tablet VALUES('zone1-0000000103','localhost',7706,'ks','0','zone1',2,'0001-01-01 00:00:00+00:00','');`,
			},
			instancesRequired: []string{"zone1-0000000103", "zone1-0000000100"},
		},
	}

	// wait for the forgetAliases cache to be initialized to prevent data race.
	waitForCacheInitialization()

	// We are setting InstancePollSeconds to 59 minutes, just for the test.
	oldVal := config.Config.InstancePollSeconds
	oldCache := forgetAliases
	defer func() {
		forgetAliases = oldCache
		config.Config.InstancePollSeconds = oldVal
	}()
	config.Config.InstancePollSeconds = 60 * 25
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
ROUND((JULIANDAY(now()) - JULIANDAY(last_checked)) * 86400) AS difference,
last_attempted_check <= last_checked as use1,
last_checked < now() - interval 1500 second as is_outdated1,
last_checked < now() - interval 3000 second as is_outdated2
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
		conditionToCheck string
	}{
		{
			name:             "Verify updated last checked",
			tabletAlias:      "zone1-0000000100",
			partialSuccess:   false,
			conditionToCheck: "last_checked >= now() - interval 30 second and last_check_partial_success = false",
		}, {
			name:             "Verify partial success",
			tabletAlias:      "zone1-0000000100",
			partialSuccess:   true,
			conditionToCheck: "last_checked >= now() - interval 30 second and last_check_partial_success = true",
		}, {
			name:           "Verify no error on unknown tablet",
			tabletAlias:    "unknown tablet",
			partialSuccess: true,
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
			err := UpdateInstanceLastChecked(tt.tabletAlias, tt.partialSuccess)
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
			conditionToCheck: "last_attempted_check >= now() - interval 30 second",
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

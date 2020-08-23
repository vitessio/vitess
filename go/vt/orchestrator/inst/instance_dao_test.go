package inst

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"testing"

	test "vitess.io/vitess/go/vt/orchestrator/external/golib/tests"
)

var (
	i710k = InstanceKey{Hostname: "i710", Port: 3306}
	i720k = InstanceKey{Hostname: "i720", Port: 3306}
	i730k = InstanceKey{Hostname: "i730", Port: 3306}
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
	i710 := Instance{Key: i710k, ServerID: 710, ExecBinlogCoordinates: BinlogCoordinates{LogFile: "mysql.000007", LogPos: 10}}
	i720 := Instance{Key: i720k, ServerID: 720, ExecBinlogCoordinates: BinlogCoordinates{LogFile: "mysql.000007", LogPos: 20}}
	i730 := Instance{Key: i730k, ServerID: 730, ExecBinlogCoordinates: BinlogCoordinates{LogFile: "mysql.000007", LogPos: 30}}
	instances := []*Instance{&i710, &i720, &i730}
	for _, instance := range instances {
		instance.Version = "5.6.7"
		instance.VersionComment = "MySQL"
		instance.Binlog_format = "STATEMENT"
		instance.BinlogRowImage = "FULL"
	}
	return instances
}

func TestMkInsertOdkuSingle(t *testing.T) {
	instances := mkTestInstances()

	sql, args, err := mkInsertOdkuForInstances(nil, true, true)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(sql, "")
	test.S(t).ExpectEquals(len(args), 0)

	// one instance
	s1 := `INSERT ignore INTO database_instance
                (hostname, port, last_checked, last_attempted_check, last_check_partial_success, uptime, server_id, server_uuid,
									version, major_version, version_comment, binlog_server, read_only, binlog_format,
									binlog_row_image, log_bin, log_slave_updates, binary_log_file, binary_log_pos, master_host, master_port,
									slave_sql_running, slave_io_running, replication_sql_thread_state, replication_io_thread_state, has_replication_filters, supports_oracle_gtid, oracle_gtid, master_uuid, ancestry_uuid, executed_gtid_set, gtid_mode, gtid_purged, gtid_errant, mariadb_gtid, pseudo_gtid,
									master_log_file, read_master_log_pos, relay_master_log_file, exec_master_log_pos, relay_log_file, relay_log_pos, last_sql_error, last_io_error, seconds_behind_master, slave_lag_seconds, sql_delay, num_slave_hosts, slave_hosts, cluster_name, suggested_cluster_alias, data_center, region, physical_environment, replication_depth, is_co_master, replication_credentials_available, has_replication_credentials, allow_tls, semi_sync_enforced, semi_sync_available, semi_sync_master_enabled, semi_sync_master_timeout, semi_sync_master_wait_for_slave_count, semi_sync_replica_enabled, semi_sync_master_status, semi_sync_master_clients, semi_sync_replica_status, instance_alias, last_discovery_latency, replication_group_name, replication_group_is_single_primary_mode, replication_group_member_state, replication_group_member_role, replication_group_members, replication_group_primary_host, replication_group_primary_port, last_seen)
        VALUES
                (?, ?, NOW(), NOW(), 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
        ON DUPLICATE KEY UPDATE
                hostname=VALUES(hostname), port=VALUES(port), last_checked=VALUES(last_checked), last_attempted_check=VALUES(last_attempted_check), last_check_partial_success=VALUES(last_check_partial_success), uptime=VALUES(uptime), server_id=VALUES(server_id), server_uuid=VALUES(server_uuid), version=VALUES(version), major_version=VALUES(major_version), version_comment=VALUES(version_comment), binlog_server=VALUES(binlog_server), read_only=VALUES(read_only), binlog_format=VALUES(binlog_format), binlog_row_image=VALUES(binlog_row_image), log_bin=VALUES(log_bin), log_slave_updates=VALUES(log_slave_updates), binary_log_file=VALUES(binary_log_file), binary_log_pos=VALUES(binary_log_pos), master_host=VALUES(master_host), master_port=VALUES(master_port), slave_sql_running=VALUES(slave_sql_running), slave_io_running=VALUES(slave_io_running), replication_sql_thread_state=VALUES(replication_sql_thread_state), replication_io_thread_state=VALUES(replication_io_thread_state), has_replication_filters=VALUES(has_replication_filters), supports_oracle_gtid=VALUES(supports_oracle_gtid), oracle_gtid=VALUES(oracle_gtid), master_uuid=VALUES(master_uuid), ancestry_uuid=VALUES(ancestry_uuid), executed_gtid_set=VALUES(executed_gtid_set), gtid_mode=VALUES(gtid_mode), gtid_purged=VALUES(gtid_purged), gtid_errant=VALUES(gtid_errant), mariadb_gtid=VALUES(mariadb_gtid), pseudo_gtid=VALUES(pseudo_gtid), master_log_file=VALUES(master_log_file), read_master_log_pos=VALUES(read_master_log_pos), relay_master_log_file=VALUES(relay_master_log_file), exec_master_log_pos=VALUES(exec_master_log_pos), relay_log_file=VALUES(relay_log_file), relay_log_pos=VALUES(relay_log_pos), last_sql_error=VALUES(last_sql_error), last_io_error=VALUES(last_io_error), seconds_behind_master=VALUES(seconds_behind_master), slave_lag_seconds=VALUES(slave_lag_seconds), sql_delay=VALUES(sql_delay), num_slave_hosts=VALUES(num_slave_hosts), slave_hosts=VALUES(slave_hosts), cluster_name=VALUES(cluster_name), suggested_cluster_alias=VALUES(suggested_cluster_alias), data_center=VALUES(data_center), region=VALUES(region), physical_environment=VALUES(physical_environment), replication_depth=VALUES(replication_depth), is_co_master=VALUES(is_co_master), replication_credentials_available=VALUES(replication_credentials_available), has_replication_credentials=VALUES(has_replication_credentials), allow_tls=VALUES(allow_tls),
								semi_sync_enforced=VALUES(semi_sync_enforced), semi_sync_available=VALUES(semi_sync_available), semi_sync_master_enabled=VALUES(semi_sync_master_enabled), semi_sync_master_timeout=VALUES(semi_sync_master_timeout), semi_sync_master_wait_for_slave_count=VALUES(semi_sync_master_wait_for_slave_count), semi_sync_replica_enabled=VALUES(semi_sync_replica_enabled), semi_sync_master_status=VALUES(semi_sync_master_status), semi_sync_master_clients=VALUES(semi_sync_master_clients), semi_sync_replica_status=VALUES(semi_sync_replica_status),
								instance_alias=VALUES(instance_alias), last_discovery_latency=VALUES(last_discovery_latency), replication_group_name=VALUES(replication_group_name), replication_group_is_single_primary_mode=VALUES(replication_group_is_single_primary_mode), replication_group_member_state=VALUES(replication_group_member_state), replication_group_member_role=VALUES(replication_group_member_role), replication_group_members=VALUES(replication_group_members), replication_group_primary_host=VALUES(replication_group_primary_host), replication_group_primary_port=VALUES(replication_group_primary_port), last_seen=VALUES(last_seen)
        `
	a1 := `i710, 3306, 0, 710, , 5.6.7, 5.6, MySQL, false, false, STATEMENT,
	FULL, false, false, , 0, , 0,
	false, false, 0, 0, false, false, false, , , , , , , false, false, , 0, mysql.000007, 10, , 0, , , {0 false}, {0 false}, 0, 0, [], , , , , , 0, false, false, false, false, false, false, false, 0, 0, false, false, 0, false, , 0, , false, , , [], , 0, `

	sql1, args1, err := mkInsertOdkuForInstances(instances[:1], false, true)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(normalizeQuery(sql1), normalizeQuery(s1))
	test.S(t).ExpectEquals(stripSpaces(fmtArgs(args1)), stripSpaces(a1))
}

func TestMkInsertOdkuThree(t *testing.T) {
	instances := mkTestInstances()

	// three instances
	s3 := `INSERT  INTO database_instance
                (hostname, port, last_checked, last_attempted_check, last_check_partial_success, uptime, server_id, server_uuid, version, major_version, version_comment, binlog_server, read_only, binlog_format, binlog_row_image, log_bin, log_slave_updates, binary_log_file, binary_log_pos, master_host, master_port, slave_sql_running, slave_io_running, replication_sql_thread_state, replication_io_thread_state, has_replication_filters, supports_oracle_gtid, oracle_gtid, master_uuid, ancestry_uuid, executed_gtid_set, gtid_mode, gtid_purged, gtid_errant, mariadb_gtid, pseudo_gtid, master_log_file, read_master_log_pos, relay_master_log_file, exec_master_log_pos, relay_log_file, relay_log_pos, last_sql_error, last_io_error, seconds_behind_master, slave_lag_seconds, sql_delay, num_slave_hosts, slave_hosts, cluster_name, suggested_cluster_alias, data_center, region, physical_environment, replication_depth, is_co_master, replication_credentials_available, has_replication_credentials, allow_tls, semi_sync_enforced, semi_sync_available, semi_sync_master_enabled, semi_sync_master_timeout, semi_sync_master_wait_for_slave_count,
								semi_sync_replica_enabled, semi_sync_master_status, semi_sync_master_clients, semi_sync_replica_status, instance_alias, last_discovery_latency, replication_group_name, replication_group_is_single_primary_mode, replication_group_member_state, replication_group_member_role, replication_group_members, replication_group_primary_host, replication_group_primary_port, last_seen)
        VALUES
								(?, ?, NOW(), NOW(), 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW()),
								(?, ?, NOW(), NOW(), 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW()),
								(?, ?, NOW(), NOW(), 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
        ON DUPLICATE KEY UPDATE
                hostname=VALUES(hostname), port=VALUES(port), last_checked=VALUES(last_checked), last_attempted_check=VALUES(last_attempted_check), last_check_partial_success=VALUES(last_check_partial_success), uptime=VALUES(uptime), server_id=VALUES(server_id), server_uuid=VALUES(server_uuid), version=VALUES(version), major_version=VALUES(major_version), version_comment=VALUES(version_comment), binlog_server=VALUES(binlog_server), read_only=VALUES(read_only), binlog_format=VALUES(binlog_format), binlog_row_image=VALUES(binlog_row_image), log_bin=VALUES(log_bin), log_slave_updates=VALUES(log_slave_updates), binary_log_file=VALUES(binary_log_file), binary_log_pos=VALUES(binary_log_pos), master_host=VALUES(master_host), master_port=VALUES(master_port), slave_sql_running=VALUES(slave_sql_running), slave_io_running=VALUES(slave_io_running), replication_sql_thread_state=VALUES(replication_sql_thread_state), replication_io_thread_state=VALUES(replication_io_thread_state), has_replication_filters=VALUES(has_replication_filters), supports_oracle_gtid=VALUES(supports_oracle_gtid), oracle_gtid=VALUES(oracle_gtid), master_uuid=VALUES(master_uuid), ancestry_uuid=VALUES(ancestry_uuid), executed_gtid_set=VALUES(executed_gtid_set), gtid_mode=VALUES(gtid_mode), gtid_purged=VALUES(gtid_purged), gtid_errant=VALUES(gtid_errant), mariadb_gtid=VALUES(mariadb_gtid), pseudo_gtid=VALUES(pseudo_gtid), master_log_file=VALUES(master_log_file), read_master_log_pos=VALUES(read_master_log_pos), relay_master_log_file=VALUES(relay_master_log_file), exec_master_log_pos=VALUES(exec_master_log_pos), relay_log_file=VALUES(relay_log_file), relay_log_pos=VALUES(relay_log_pos), last_sql_error=VALUES(last_sql_error), last_io_error=VALUES(last_io_error), seconds_behind_master=VALUES(seconds_behind_master), slave_lag_seconds=VALUES(slave_lag_seconds), sql_delay=VALUES(sql_delay), num_slave_hosts=VALUES(num_slave_hosts), slave_hosts=VALUES(slave_hosts), cluster_name=VALUES(cluster_name), suggested_cluster_alias=VALUES(suggested_cluster_alias), data_center=VALUES(data_center), region=VALUES(region),
								physical_environment=VALUES(physical_environment), replication_depth=VALUES(replication_depth), is_co_master=VALUES(is_co_master), replication_credentials_available=VALUES(replication_credentials_available), has_replication_credentials=VALUES(has_replication_credentials), allow_tls=VALUES(allow_tls), semi_sync_enforced=VALUES(semi_sync_enforced), semi_sync_available=VALUES(semi_sync_available),
								semi_sync_master_enabled=VALUES(semi_sync_master_enabled), semi_sync_master_timeout=VALUES(semi_sync_master_timeout), semi_sync_master_wait_for_slave_count=VALUES(semi_sync_master_wait_for_slave_count), semi_sync_replica_enabled=VALUES(semi_sync_replica_enabled), semi_sync_master_status=VALUES(semi_sync_master_status), semi_sync_master_clients=VALUES(semi_sync_master_clients), semi_sync_replica_status=VALUES(semi_sync_replica_status),
								instance_alias=VALUES(instance_alias), last_discovery_latency=VALUES(last_discovery_latency), replication_group_name=VALUES(replication_group_name), replication_group_is_single_primary_mode=VALUES(replication_group_is_single_primary_mode), replication_group_member_state=VALUES(replication_group_member_state), replication_group_member_role=VALUES(replication_group_member_role), replication_group_members=VALUES(replication_group_members), replication_group_primary_host=VALUES(replication_group_primary_host), replication_group_primary_port=VALUES(replication_group_primary_port), last_seen=VALUES(last_seen)
        `
	a3 := `
		i710, 3306, 0, 710, , 5.6.7, 5.6, MySQL, false, false, STATEMENT, FULL, false, false, , 0, , 0, false, false, 0, 0, false, false, false, , , , , , , false, false, , 0, mysql.000007, 10, , 0, , , {0 false}, {0 false}, 0, 0, [], , , , , , 0, false, false, false, false, false, false, false, 0, 0, false, false, 0, false, , 0, , false, , , [], , 0,
		i720, 3306, 0, 720, , 5.6.7, 5.6, MySQL, false, false, STATEMENT, FULL, false, false, , 0, , 0, false, false, 0, 0, false, false, false, , , , , , , false, false, , 0, mysql.000007, 20, , 0, , , {0 false}, {0 false}, 0, 0, [], , , , , , 0, false, false, false, false, false, false, false, 0, 0, false, false, 0, false, , 0, , false, , , [], , 0,
		i730, 3306, 0, 730, , 5.6.7, 5.6, MySQL, false, false, STATEMENT, FULL, false, false, , 0, , 0, false, false, 0, 0, false, false, false, , , , , , , false, false, , 0, mysql.000007, 30, , 0, , , {0 false}, {0 false}, 0, 0, [], , , , , , 0, false, false, false, false, false, false, false, 0, 0, false, false, 0, false, , 0, , false, , , [], , 0,
		`

	sql3, args3, err := mkInsertOdkuForInstances(instances[:3], true, true)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(normalizeQuery(sql3), normalizeQuery(s3))
	test.S(t).ExpectEquals(stripSpaces(fmtArgs(args3)), stripSpaces(a3))
}

func fmtArgs(args []interface{}) string {
	b := &bytes.Buffer{}
	for _, a := range args {
		fmt.Fprint(b, a)
		fmt.Fprint(b, ", ")
	}
	return b.String()
}

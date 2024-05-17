/*
Copyright 2019 The Vitess Authors.

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
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"vitess.io/vitess/go/mysql/capabilities"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/vt/proto/replicationdata"
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// mysqlFlavor implements the Flavor interface for Mysql. This is
// the most up to date / recent flavor and uses the most modern
// replication commands and semantics.
type mysqlFlavor struct {
	serverVersion string
}

// mysqlFlavor8 is for later MySQL 8.0 versions. It's the same as
// the modern flavor, but overrides some specific commands that
// are only available on MySQL 8.2.0 and later. This is specifically
// commands like SHOW BINARY LOG STATUS.
type mysqlFlavor8 struct {
	mysqlFlavor
}

// mysqlFlavor82 is for MySQL 8.2.0 and later. It's the most modern
// flavor but has an explicit name so that it's clear it's explicitly
// for MySQL 8.2.0 and later.
type mysqlFlavor82 struct {
	mysqlFlavor
}

var _ flavor = (*mysqlFlavor8)(nil)
var _ flavor = (*mysqlFlavor82)(nil)

// primaryGTIDSet is part of the Flavor interface.
func (mysqlFlavor) primaryGTIDSet(c *Conn) (replication.GTIDSet, error) {
	// keep @@global as lowercase, as some servers like the Ripple binlog server only honors a lowercase `global` value
	qr, err := c.ExecuteFetch("SELECT @@global.gtid_executed", 1, false)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected result format for gtid_executed: %#v", qr)
	}
	return replication.ParseMysql56GTIDSet(qr.Rows[0][0].ToString())
}

// purgedGTIDSet is part of the Flavor interface.
func (mysqlFlavor) purgedGTIDSet(c *Conn) (replication.GTIDSet, error) {
	// keep @@global as lowercase, as some servers like the Ripple binlog server only honors a lowercase `global` value
	qr, err := c.ExecuteFetch("SELECT @@global.gtid_purged", 1, false)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected result format for gtid_purged: %#v", qr)
	}
	return replication.ParseMysql56GTIDSet(qr.Rows[0][0].ToString())
}

// serverUUID is part of the Flavor interface.
func (mysqlFlavor) serverUUID(c *Conn) (string, error) {
	// keep @@global as lowercase, as some servers like the Ripple binlog server only honors a lowercase `global` value
	qr, err := c.ExecuteFetch("SELECT @@global.server_uuid", 1, false)
	if err != nil {
		return "", err
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
		return "", vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected result format for server_uuid: %#v", qr)
	}
	return qr.Rows[0][0].ToString(), nil
}

// gtidMode is part of the Flavor interface.
func (mysqlFlavor) gtidMode(c *Conn) (string, error) {
	qr, err := c.ExecuteFetch("select @@global.gtid_mode", 1, false)
	if err != nil {
		return "", err
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
		return "", vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected result format for gtid_mode: %#v", qr)
	}
	return qr.Rows[0][0].ToString(), nil
}

func (f mysqlFlavor) startReplicationCommand() string {
	return "START REPLICA"
}

func (f mysqlFlavor) restartReplicationCommands() []string {
	return []string{
		"STOP REPLICA",
		"RESET REPLICA",
		"START REPLICA",
	}
}

func (f mysqlFlavor) startReplicationUntilAfter(pos replication.Position) string {
	return fmt.Sprintf("START REPLICA UNTIL SQL_AFTER_GTIDS = '%s'", pos)
}

func (f mysqlFlavor) startSQLThreadUntilAfter(pos replication.Position) string {
	return fmt.Sprintf("START REPLICA SQL_THREAD UNTIL SQL_AFTER_GTIDS = '%s'", pos)
}

func (f mysqlFlavor) stopReplicationCommand() string {
	return "STOP REPLICA"
}

func (f mysqlFlavor) resetReplicationCommand() string {
	return "RESET REPLICA ALL"
}

func (f mysqlFlavor) stopIOThreadCommand() string {
	return "STOP REPLICA IO_THREAD"
}

func (f mysqlFlavor) stopSQLThreadCommand() string {
	return "STOP REPLICA SQL_THREAD"
}

func (f mysqlFlavor) startSQLThreadCommand() string {
	return "START REPLICA SQL_THREAD"
}

// resetReplicationCommands is part of the Flavor interface.
func (mysqlFlavor) resetReplicationCommands(c *Conn) []string {
	resetCommands := []string{
		"STOP REPLICA",
		"RESET REPLICA ALL",           // "ALL" makes it forget source host:port.
		"RESET BINARY LOGS AND GTIDS", // This will also clear gtid_executed and gtid_purged.
	}
	status, err := c.SemiSyncExtensionLoaded()
	if err != nil {
		return resetCommands
	}
	switch status {
	case SemiSyncTypeSource:
		resetCommands = append(resetCommands, "SET GLOBAL rpl_semi_sync_source_enabled = false, GLOBAL rpl_semi_sync_replica_enabled = false") // semi-sync will be enabled if needed when replica is started.
	case SemiSyncTypeMaster:
		resetCommands = append(resetCommands, "SET GLOBAL rpl_semi_sync_master_enabled = false, GLOBAL rpl_semi_sync_slave_enabled = false") // semi-sync will be enabled if needed when replica is started.
	default:
		// Nothing to do.
	}
	return resetCommands
}

func (mysqlFlavor) resetBinaryLogsCommand() string {
	return "RESET BINARY LOGS AND GTIDS"
}

// resetReplicationCommands is part of the Flavor interface.
func (mysqlFlavor8) resetReplicationCommands(c *Conn) []string {
	resetCommands := []string{
		"STOP REPLICA",
		"RESET REPLICA ALL", // "ALL" makes it forget source host:port.
		"RESET MASTER",      // This will also clear gtid_executed and gtid_purged.
	}
	status, err := c.SemiSyncExtensionLoaded()
	if err != nil {
		return resetCommands
	}
	switch status {
	case SemiSyncTypeSource:
		resetCommands = append(resetCommands, "SET GLOBAL rpl_semi_sync_source_enabled = false, GLOBAL rpl_semi_sync_replica_enabled = false") // semi-sync will be enabled if needed when replica is started.
	case SemiSyncTypeMaster:
		resetCommands = append(resetCommands, "SET GLOBAL rpl_semi_sync_master_enabled = false, GLOBAL rpl_semi_sync_slave_enabled = false") // semi-sync will be enabled if needed when replica is started.
	default:
		// Nothing to do.
	}
	return resetCommands
}

// resetReplicationCommands is part of the Flavor interface.
func (mysqlFlavor8) resetBinaryLogsCommand() string {
	return "RESET MASTER"
}

// resetReplicationParametersCommands is part of the Flavor interface.
func (mysqlFlavor) resetReplicationParametersCommands(c *Conn) []string {
	resetCommands := []string{
		"RESET REPLICA ALL", // "ALL" makes it forget source host:port.
	}
	return resetCommands
}

// sendBinlogDumpCommand is part of the Flavor interface.
func (mysqlFlavor) sendBinlogDumpCommand(c *Conn, serverID uint32, binlogFilename string, startPos replication.Position) error {
	gtidSet, ok := startPos.GTIDSet.(replication.Mysql56GTIDSet)
	if !ok {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "startPos.GTIDSet is wrong type - expected Mysql56GTIDSet, got: %#v", startPos.GTIDSet)
	}

	// Build the command.
	sidBlock := gtidSet.SIDBlock()
	return c.WriteComBinlogDumpGTID(serverID, binlogFilename, 4, 0, sidBlock)
}

// setReplicationPositionCommands is part of the Flavor interface.
func (mysqlFlavor8) setReplicationPositionCommands(pos replication.Position) []string {
	return []string{
		"RESET MASTER", // We must clear gtid_executed before setting gtid_purged.
		fmt.Sprintf("SET GLOBAL gtid_purged = '%s'", pos),
	}
}

// setReplicationPositionCommands is part of the Flavor interface.
func (mysqlFlavor) setReplicationPositionCommands(pos replication.Position) []string {
	return []string{
		"RESET BINARY LOGS AND GTIDS", // We must clear gtid_executed before setting gtid_purged.
		fmt.Sprintf("SET GLOBAL gtid_purged = '%s'", pos),
	}
}

// primaryStatus is part of the Flavor interface.
func (mysqlFlavor8) primaryStatus(c *Conn) (replication.PrimaryStatus, error) {
	qr, err := c.ExecuteFetch("SHOW MASTER STATUS", 100, true /* wantfields */)
	if err != nil {
		return replication.PrimaryStatus{}, err
	}
	if len(qr.Rows) == 0 {
		// The query returned no data. We don't know how this could happen.
		return replication.PrimaryStatus{}, ErrNoPrimaryStatus
	}

	resultMap, err := resultToMap(qr)
	if err != nil {
		return replication.PrimaryStatus{}, err
	}

	return replication.ParseMysqlPrimaryStatus(resultMap)
}

// primaryStatus is part of the Flavor interface.
func (mysqlFlavor) primaryStatus(c *Conn) (replication.PrimaryStatus, error) {
	qr, err := c.ExecuteFetch("SHOW BINARY LOG STATUS", 100, true /* wantfields */)
	if err != nil {
		return replication.PrimaryStatus{}, err
	}
	if len(qr.Rows) == 0 {
		// The query returned no data. We don't know how this could happen.
		return replication.PrimaryStatus{}, ErrNoPrimaryStatus
	}

	resultMap, err := resultToMap(qr)
	if err != nil {
		return replication.PrimaryStatus{}, err
	}

	return replication.ParseMysqlPrimaryStatus(resultMap)
}

// replicationConfiguration is part of the Flavor interface.
func (mysqlFlavor) replicationConfiguration(c *Conn) (*replicationdata.Configuration, error) {
	qr, err := c.ExecuteFetch(readReplicationConnectionConfiguration, 100, true /* wantfields */)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) == 0 {
		// The query returned no data. This is not a replica.
		return nil, ErrNotReplica
	}

	resultMap, err := resultToMap(qr)
	if err != nil {
		return nil, err
	}

	heartbeatInterval, err := strconv.ParseFloat(resultMap["HEARTBEAT_INTERVAL"], 64)
	if err != nil {
		return nil, err
	}

	return &replicationdata.Configuration{
		HeartbeatInterval: heartbeatInterval,
	}, nil
}

// replicationNetTimeout is part of the Flavor interface.
func (mysqlFlavor) replicationNetTimeout(c *Conn) (int32, error) {
	qr, err := c.ExecuteFetch("select @@global.replica_net_timeout", 1, false)
	if err != nil {
		return 0, err
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
		return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected result format for replica_net_timeout: %#v", qr)
	}
	return qr.Rows[0][0].ToInt32()
}

// status is part of the Flavor interface.
func (mysqlFlavor) status(c *Conn) (replication.ReplicationStatus, error) {
	qr, err := c.ExecuteFetch("SHOW REPLICA STATUS", 100, true /* wantfields */)
	if err != nil {
		return replication.ReplicationStatus{}, err
	}
	if len(qr.Rows) == 0 {
		// The query returned no data, meaning the server
		// is not configured as a replica.
		return replication.ReplicationStatus{}, ErrNotReplica
	}

	resultMap, err := resultToMap(qr)
	if err != nil {
		return replication.ReplicationStatus{}, err
	}

	return replication.ParseMysqlReplicationStatus(resultMap, true)
}

// waitUntilPosition is part of the Flavor interface.
func (mysqlFlavor) waitUntilPosition(ctx context.Context, c *Conn, pos replication.Position) error {
	// A timeout of 0 means wait indefinitely.
	timeoutSeconds := 0
	if deadline, ok := ctx.Deadline(); ok {
		timeout := time.Until(deadline)
		if timeout <= 0 {
			return vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "timed out waiting for position %v", pos)
		}

		// Only whole numbers of seconds are supported.
		timeoutSeconds = int(timeout.Seconds())
		if timeoutSeconds == 0 {
			// We don't want a timeout <1.0s to truncate down to become infinite.
			timeoutSeconds = 1
		}
	}

	query := fmt.Sprintf("SELECT WAIT_FOR_EXECUTED_GTID_SET('%s', %v)", pos, timeoutSeconds)
	result, err := c.ExecuteFetch(query, 1, false)
	if err != nil {
		return err
	}

	// For WAIT_FOR_EXECUTED_GTID_SET(), the return value is the state of the query, where
	// 0 represents success, and 1 represents timeout. Any other failures generate an error.
	if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid results: %#v", result)
	}
	val := result.Rows[0][0]
	state, err := val.ToInt64()
	if err != nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid result of %#v", val)
	}
	switch state {
	case 0:
		return nil
	case 1:
		return vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "timed out waiting for position %v", pos)
	default:
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid result of %d", state)
	}
}

// readBinlogEvent is part of the Flavor interface.
func (mysqlFlavor) readBinlogEvent(c *Conn) (BinlogEvent, error) {
	result, err := c.ReadPacket()
	if err != nil {
		return nil, err
	}
	switch result[0] {
	case EOFPacket:
		return nil, sqlerror.NewSQLError(sqlerror.CRServerLost, sqlerror.SSUnknownSQLState, "%v", io.EOF)
	case ErrPacket:
		return nil, ParseErrorPacket(result)
	}
	buf, semiSyncAckRequested, err := c.AnalyzeSemiSyncAckRequest(result[1:])
	if err != nil {
		return nil, err
	}
	ev := NewMysql56BinlogEventWithSemiSyncInfo(buf, semiSyncAckRequested)
	return ev, nil
}

// baseShowTables is part of the Flavor interface.
func (mysqlFlavor) baseShowTables() string {
	return "SELECT table_name, table_type, unix_timestamp(create_time), table_comment FROM information_schema.tables WHERE table_schema = database()"
}

// TablesWithSize80 is a query to select table along with size for mysql 8.0
//
// Note the following:
//   - We use a single query to fetch both partitioned and non-partitioned tables. This is because
//     accessing `information_schema.innodb_tablespaces` is expensive on servers with many tablespaces,
//     and every query that loads the table needs to perform full table scans on it. Doing a single
//     table scan is more efficient than doing more than one.
//   - We utilize `INFORMATION_SCHEMA`.`TABLES`.`CREATE_OPTIONS` column to do early pruning before the JOIN.
//   - `TABLES`.`TABLE_NAME` has `utf8mb4_0900_ai_ci` collation.  `INNODB_TABLESPACES`.`NAME` has `utf8mb3_general_ci`.
//     We normalize the collation to get better query performance (we force the casting at the time of our choosing)
const TablesWithSize80 = `SELECT t.table_name,
		t.table_type,
		UNIX_TIMESTAMP(t.create_time),
		t.table_comment,
		SUM(i.file_size),
		SUM(i.allocated_size)
	FROM information_schema.tables t
		LEFT JOIN information_schema.innodb_tablespaces i
	ON i.name LIKE CONCAT(t.table_schema, '/', t.table_name, IF(t.create_options <=> 'partitioned', '#p#%', '')) COLLATE utf8mb3_general_ci
	WHERE
		t.table_schema = database()
	GROUP BY
		t.table_schema, t.table_name, t.table_type, t.create_time, t.table_comment
`

// baseShowTablesWithSizes is part of the Flavor interface.
func (mysqlFlavor57) baseShowTablesWithSizes() string {
	return TablesWithSize57
}

// supportsCapability is part of the Flavor interface.
func (f mysqlFlavor) supportsCapability(capability capabilities.FlavorCapability) (bool, error) {
	return capabilities.MySQLVersionHasCapability(f.serverVersion, capability)
}

// baseShowTablesWithSizes is part of the Flavor interface.
func (mysqlFlavor) baseShowTablesWithSizes() string {
	return TablesWithSize80
}

func (mysqlFlavor) setReplicationSourceCommand(params *ConnParams, host string, port int32, heartbeatInterval float64, connectRetry int) string {
	args := []string{
		fmt.Sprintf("SOURCE_HOST = '%s'", host),
		fmt.Sprintf("SOURCE_PORT = %d", port),
		fmt.Sprintf("SOURCE_USER = '%s'", params.Uname),
		fmt.Sprintf("SOURCE_PASSWORD = '%s'", params.Pass),
		fmt.Sprintf("SOURCE_CONNECT_RETRY = %d", connectRetry),
	}
	if params.SslEnabled() {
		args = append(args, "SOURCE_SSL = 1")
	}
	if params.SslCa != "" {
		args = append(args, fmt.Sprintf("SOURCE_SSL_CA = '%s'", params.SslCa))
	}
	if params.SslCaPath != "" {
		args = append(args, fmt.Sprintf("SOURCE_SSL_CAPATH = '%s'", params.SslCaPath))
	}
	if params.SslCert != "" {
		args = append(args, fmt.Sprintf("SOURCE_SSL_CERT = '%s'", params.SslCert))
	}
	if params.SslKey != "" {
		args = append(args, fmt.Sprintf("SOURCE_SSL_KEY = '%s'", params.SslKey))
	}
	if heartbeatInterval != 0 {
		args = append(args, fmt.Sprintf("SOURCE_HEARTBEAT_PERIOD = %v", heartbeatInterval))
	}
	args = append(args, "SOURCE_AUTO_POSITION = 1")
	return "CHANGE REPLICATION SOURCE TO\n  " + strings.Join(args, ",\n  ")
}

func (mysqlFlavor) catchupToGTIDCommands(params *ConnParams, replPos replication.Position) []string {
	cmds := []string{
		"STOP REPLICA FOR CHANNEL '' ",
		"STOP REPLICA IO_THREAD FOR CHANNEL ''",
	}

	if params.SslCa != "" || params.SslCert != "" {
		// We need to use TLS
		cmd := fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_HOST='%s', SOURCE_PORT=%d, SOURCE_USER='%s', SOURCE_PASSWORD='%s', SOURCE_AUTO_POSITION=1, SOURCE_SSL=1", params.Host, params.Port, params.Uname, params.Pass)
		if params.SslCa != "" {
			cmd += fmt.Sprintf(", SOURCE_SSL_CA='%s'", params.SslCa)
		}
		if params.SslCert != "" {
			cmd += fmt.Sprintf(", SOURCE_SSL_CERT='%s'", params.SslCert)
		}
		if params.SslKey != "" {
			cmd += fmt.Sprintf(", SOURCE_SSL_KEY='%s'", params.SslKey)
		}
		cmds = append(cmds, cmd+";")
	} else {
		// No TLS
		cmds = append(cmds, fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_HOST='%s', SOURCE_PORT=%d, SOURCE_USER='%s', SOURCE_PASSWORD='%s', SOURCE_AUTO_POSITION=1;", params.Host, params.Port, params.Uname, params.Pass))
	}

	if replPos.IsZero() { // when the there is no afterPos, that means need to replicate completely
		cmds = append(cmds, "START REPLICA")
	} else {
		cmds = append(cmds, fmt.Sprintf("START REPLICA UNTIL SQL_BEFORE_GTIDS = '%s'", replPos.GTIDSet.Last()))
	}
	return cmds
}

func (mysqlFlavor) binlogReplicatedUpdates() string {
	return "@@global.log_replica_updates"
}

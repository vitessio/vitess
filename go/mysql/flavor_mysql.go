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
	// keep @@global as lowercase, as some servers like a binlog server only honors a lowercase `global` value
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
	// keep @@global as lowercase, as some servers like a binlog server only honors a lowercase `global` value
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
	// keep @@global as lowercase, as some servers like a binlog server only honors a lowercase `global` value
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
	var sidBlock []byte
	if gtidSet != nil {
		sidBlock = gtidSet.SIDBlock()
	}
	var flags2 uint16
	if binlogFilename != "" {
		flags2 |= BinlogThroughPosition
	}
	if len(sidBlock) > 0 {
		flags2 |= BinlogThroughGTID
	}
	return c.WriteComBinlogDumpGTID(serverID, binlogFilename, 4, flags2, sidBlock)
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
		return nil, sqlerror.NewSQLErrorf(sqlerror.CRServerLost, sqlerror.SSUnknownSQLState, "%v", io.EOF)
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
	return BaseShowTables
}

const BaseShowTables = `SELECT t.table_name,
		t.table_type,
		UNIX_TIMESTAMP(t.create_time),
		t.table_comment
	FROM information_schema.tables t
	WHERE
		t.table_schema = database()
`

// InnoDBTableSizes: a query to return file/allocated sizes for InnoDB tables.
// File sizes and allocated sizes are found in information_schema.innodb_tablespaces
// Table names in information_schema.innodb_tablespaces match those in information_schema.tables, even for table names
// with special characters. This, a innodb_tablespaces.name could be `my-db/my-table`.
// These tablespaces will have one entry for every InnoDB table, hidden or internal. This means:
// - One entry for every partition in a partitioned table.
// - Several entries for any FULLTEXT index (FULLTEXT indexes are not BTREEs and are implemented using multiple hidden tables)
// So a single table wih a FULLTEXT index will have one entry for the "normal" table, plus multiple more entries for
// every FTS index hidden tables.
// Thankfully FULLTEXT does not work with Partitioning so this does not explode too much.
// Next thing is that FULLTEXT hidden table names do not resemble the original table name, and could look like:
// `a-b/fts_000000000000075e_00000000000005f9_index_2`.
// To unlock the identify of this table we turn to information_schema.innodb_tables. These table similarly has one entry for
// every InnoDB table, normal or hidden. It also has a `TABLE_ID` value. Given some table with FULLTEXT keys, its TABLE_ID
// is encoded in the names of the hidden tables in information_schema.innodb_tablespaces: `000000000000075e` in the
// example above.
//
// The query below is a two part:
//  1. Finding the "normal" tables only, those that the user created. We note their file size and allocated size.
//  2. Finding the hidden tables only, those that implement FTS keys. We aggregate their file size and allocated size grouping
//     by the original table name with which they're associated.
//
// A table that has a FULLTEXT index will have two entries in the result set:
// - one for the "normal" table size (actual rows, texts, etc.)
// - and one for the aggregated hidden table size
// The code that reads the results of this query will need to add the two.
// Similarly, the code will need to know how to aggregate the sizes of partitioned tables, which could appear as:
// - `mydb/tbl_part#p#p0`
// - `mydb/tbl_part#p#p1`
// - `mydb/tbl_part#p#p2`
// - `mydb/tbl_part#p#p3`
//
// Lastly, we note that table name in information_schema.innodb_tables are encoded. A table that shows as
// `my-db/my-table` in information_schema.innodb_tablespaces will show as `my@002ddb/my@002dtable` in information_schema.innodb_tables.
// So this query returns InnoDB-encoded table names. The golang code reading those will have to decode the names.
const InnoDBTableSizes = `
	SELECT
		it.name,
		its.file_size as normal_tables_sum_file_size,
		its.allocated_size as normal_tables_sum_allocated_size
	FROM
		information_schema.innodb_tables it
		JOIN information_schema.innodb_tablespaces its
		ON (its.space = it.space)
		WHERE
					its.name LIKE CONCAT(database(), '/%')
			AND	its.name NOT LIKE CONCAT(database(), '/fts_%')
	UNION ALL
	SELECT
		it.name,
		SUM(its.file_size) as hidden_tables_sum_file_size,
		SUM(its.allocated_size) as hidden_tables_sum_allocated_size
	FROM
		information_schema.innodb_tables it
		JOIN information_schema.innodb_tablespaces its
		ON (
				 its.name LIKE CONCAT(database(), '/fts_', CONVERT(LPAD(HEX(table_id), 16, '0') USING utf8mb3) COLLATE utf8mb3_general_ci, '_%')
		)
		WHERE
				 its.name LIKE CONCAT(database(), '/fts_%')
		GROUP BY it.name
`

const ShowPartitons = `select table_name, partition_name from information_schema.partitions where table_schema = database() and partition_name is not null`
const ShowTableRowCountClusteredIndex = `select table_name, n_rows, clustered_index_size * @@innodb_page_size from mysql.innodb_table_stats where database_name = database()`
const ShowIndexSizes = `select table_name, index_name, stat_value * @@innodb_page_size from mysql.innodb_index_stats where database_name = database() and stat_name = 'size'`
const ShowIndexCardinalities = `select table_name, index_name, max(cardinality) from information_schema.statistics s where table_schema = database() group by s.table_name, s.index_name`

// baseShowTablesWithSizes is part of the Flavor interface.
func (mysqlFlavor57) baseShowTablesWithSizes() string {
	// For 5.7, we use the base query instead of the query with sizes. Flavor57 should only be used
	// for unmanaged tables during import. We don't need to know the size of the tables in that case since
	// the size information is mainly used by Online DDL.
	// The TablesWithSize57 query can be very non-performant on some external databases, for example on Aurora with
	// a large number of tables, it can time out often.
	return BaseShowTables
}

// baseShowInnodbTableSizes is part of the Flavor interface.
func (mysqlFlavor57) baseShowInnodbTableSizes() string {
	return ""
}

func (mysqlFlavor57) baseShowPartitions() string {
	return ""
}

func (mysqlFlavor57) baseShowTableRowCountClusteredIndex() string {
	return ""
}

func (mysqlFlavor57) baseShowIndexSizes() string {
	return ""
}

func (mysqlFlavor57) baseShowIndexCardinalities() string {
	return ""
}

// supportsCapability is part of the Flavor interface.
func (f mysqlFlavor) supportsCapability(capability capabilities.FlavorCapability) (bool, error) {
	return capabilities.MySQLVersionHasCapability(f.serverVersion, capability)
}

// baseShowTablesWithSizes is part of the Flavor interface.
func (mysqlFlavor) baseShowTablesWithSizes() string {
	return "" // Won't be used, as InnoDBTableSizes is defined, and schema.Engine will use that, instead.
}

// baseShowInnodbTableSizes is part of the Flavor interface.
func (mysqlFlavor) baseShowInnodbTableSizes() string {
	return InnoDBTableSizes
}

func (mysqlFlavor) baseShowPartitions() string {
	return ShowPartitons
}

func (mysqlFlavor) baseShowTableRowCountClusteredIndex() string {
	return ShowTableRowCountClusteredIndex
}

func (mysqlFlavor) baseShowIndexSizes() string {
	return ShowIndexSizes
}

func (mysqlFlavor) baseShowIndexCardinalities() string {
	return ShowIndexCardinalities
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

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
	"time"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// mysqlFlavor implements the Flavor interface for Mysql.
type mysqlFlavor struct{}
type mysqlFlavor56 struct {
	mysqlFlavor
}
type mysqlFlavor57 struct {
	mysqlFlavor
}
type mysqlFlavor80 struct {
	mysqlFlavor
}

var _ flavor = (*mysqlFlavor56)(nil)
var _ flavor = (*mysqlFlavor57)(nil)
var _ flavor = (*mysqlFlavor80)(nil)

// primaryGTIDSet is part of the Flavor interface.
func (mysqlFlavor) primaryGTIDSet(c *Conn) (GTIDSet, error) {
	// keep @@global as lowercase, as some servers like the Ripple binlog server only honors a lowercase `global` value
	qr, err := c.ExecuteFetch("SELECT @@global.gtid_executed", 1, false)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "unexpected result format for gtid_executed: %#v", qr)
	}
	return ParseMysql56GTIDSet(qr.Rows[0][0].ToString())
}

// purgedGTIDSet is part of the Flavor interface.
func (mysqlFlavor) purgedGTIDSet(c *Conn) (GTIDSet, error) {
	// keep @@global as lowercase, as some servers like the Ripple binlog server only honors a lowercase `global` value
	qr, err := c.ExecuteFetch("SELECT @@global.gtid_purged", 1, false)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "unexpected result format for gtid_purged: %#v", qr)
	}
	return ParseMysql56GTIDSet(qr.Rows[0][0].ToString())
}

// serverUUID is part of the Flavor interface.
func (mysqlFlavor) serverUUID(c *Conn) (string, error) {
	// keep @@global as lowercase, as some servers like the Ripple binlog server only honors a lowercase `global` value
	qr, err := c.ExecuteFetch("SELECT @@global.server_uuid", 1, false)
	if err != nil {
		return "", err
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
		return "", vterrors.Errorf(vtrpc.Code_INTERNAL, "unexpected result format for server_uuid: %#v", qr)
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
		return "", vterrors.Errorf(vtrpc.Code_INTERNAL, "unexpected result format for gtid_mode: %#v", qr)
	}
	return qr.Rows[0][0].ToString(), nil
}

func (mysqlFlavor) startReplicationCommand() string {
	return "START SLAVE"
}

func (mysqlFlavor) restartReplicationCommands() []string {
	return []string{
		"STOP SLAVE",
		"RESET SLAVE",
		"START SLAVE",
	}
}

func (mysqlFlavor) startReplicationUntilAfter(pos Position) string {
	return fmt.Sprintf("START SLAVE UNTIL SQL_AFTER_GTIDS = '%s'", pos)
}

func (mysqlFlavor) startSQLThreadUntilAfter(pos Position) string {
	return fmt.Sprintf("START SLAVE SQL_THREAD UNTIL SQL_AFTER_GTIDS = '%s'", pos)
}

func (mysqlFlavor) stopReplicationCommand() string {
	return "STOP SLAVE"
}

func (mysqlFlavor) stopIOThreadCommand() string {
	return "STOP SLAVE IO_THREAD"
}

func (mysqlFlavor) stopSQLThreadCommand() string {
	return "STOP SLAVE SQL_THREAD"
}

func (mysqlFlavor) startSQLThreadCommand() string {
	return "START SLAVE SQL_THREAD"
}

// sendBinlogDumpCommand is part of the Flavor interface.
func (mysqlFlavor) sendBinlogDumpCommand(c *Conn, serverID uint32, binlogFilename string, startPos Position) error {
	gtidSet, ok := startPos.GTIDSet.(Mysql56GTIDSet)
	if !ok {
		return vterrors.Errorf(vtrpc.Code_INTERNAL, "startPos.GTIDSet is wrong type - expected Mysql56GTIDSet, got: %#v", startPos.GTIDSet)
	}

	// Build the command.
	sidBlock := gtidSet.SIDBlock()
	return c.WriteComBinlogDumpGTID(serverID, binlogFilename, 4, 0, sidBlock)
}

// resetReplicationCommands is part of the Flavor interface.
func (mysqlFlavor) resetReplicationCommands(c *Conn) []string {
	resetCommands := []string{
		"STOP SLAVE",
		"RESET SLAVE ALL", // "ALL" makes it forget source host:port.
		"RESET MASTER",    // This will also clear gtid_executed and gtid_purged.
	}
	if c.SemiSyncExtensionLoaded() {
		resetCommands = append(resetCommands, "SET GLOBAL rpl_semi_sync_master_enabled = false, GLOBAL rpl_semi_sync_slave_enabled = false") // semi-sync will be enabled if needed when replica is started.
	}
	return resetCommands
}

// resetReplicationParametersCommands is part of the Flavor interface.
func (mysqlFlavor) resetReplicationParametersCommands(c *Conn) []string {
	resetCommands := []string{
		"RESET SLAVE ALL", // "ALL" makes it forget source host:port.
	}
	return resetCommands
}

// setReplicationPositionCommands is part of the Flavor interface.
func (mysqlFlavor) setReplicationPositionCommands(pos Position) []string {
	return []string{
		"RESET MASTER", // We must clear gtid_executed before setting gtid_purged.
		fmt.Sprintf("SET GLOBAL gtid_purged = '%s'", pos),
	}
}

// setReplicationPositionCommands is part of the Flavor interface.
func (mysqlFlavor) changeReplicationSourceArg() string {
	return "MASTER_AUTO_POSITION = 1"
}

// status is part of the Flavor interface.
func (mysqlFlavor) status(c *Conn) (ReplicationStatus, error) {
	qr, err := c.ExecuteFetch("SHOW SLAVE STATUS", 100, true /* wantfields */)
	if err != nil {
		return ReplicationStatus{}, err
	}
	if len(qr.Rows) == 0 {
		// The query returned no data, meaning the server
		// is not configured as a replica.
		return ReplicationStatus{}, ErrNotReplica
	}

	resultMap, err := resultToMap(qr)
	if err != nil {
		return ReplicationStatus{}, err
	}

	return parseMysqlReplicationStatus(resultMap)
}

func parseMysqlReplicationStatus(resultMap map[string]string) (ReplicationStatus, error) {
	status := parseReplicationStatus(resultMap)
	uuidString := resultMap["Master_UUID"]
	if uuidString != "" {
		sid, err := ParseSID(uuidString)
		if err != nil {
			return ReplicationStatus{}, vterrors.Wrapf(err, "cannot decode SourceUUID")
		}
		status.SourceUUID = sid
	}

	var err error
	status.Position.GTIDSet, err = ParseMysql56GTIDSet(resultMap["Executed_Gtid_Set"])
	if err != nil {
		return ReplicationStatus{}, vterrors.Wrapf(err, "ReplicationStatus can't parse MySQL 5.6 GTID (Executed_Gtid_Set: %#v)", resultMap["Executed_Gtid_Set"])
	}
	relayLogGTIDSet, err := ParseMysql56GTIDSet(resultMap["Retrieved_Gtid_Set"])
	if err != nil {
		return ReplicationStatus{}, vterrors.Wrapf(err, "ReplicationStatus can't parse MySQL 5.6 GTID (Retrieved_Gtid_Set: %#v)", resultMap["Retrieved_Gtid_Set"])
	}
	// We take the union of the executed and retrieved gtidset, because the retrieved gtidset only represents GTIDs since
	// the relay log has been reset. To get the full Position, we need to take a union of executed GTIDSets, since these would
	// have been in the relay log's GTIDSet in the past, prior to a reset.
	status.RelayLogPosition.GTIDSet = status.Position.GTIDSet.Union(relayLogGTIDSet)

	return status, nil
}

// primaryStatus is part of the Flavor interface.
func (mysqlFlavor) primaryStatus(c *Conn) (PrimaryStatus, error) {
	qr, err := c.ExecuteFetch("SHOW MASTER STATUS", 100, true /* wantfields */)
	if err != nil {
		return PrimaryStatus{}, err
	}
	if len(qr.Rows) == 0 {
		// The query returned no data. We don't know how this could happen.
		return PrimaryStatus{}, ErrNoPrimaryStatus
	}

	resultMap, err := resultToMap(qr)
	if err != nil {
		return PrimaryStatus{}, err
	}

	return parseMysqlPrimaryStatus(resultMap)
}

func parseMysqlPrimaryStatus(resultMap map[string]string) (PrimaryStatus, error) {
	status := parsePrimaryStatus(resultMap)

	var err error
	status.Position.GTIDSet, err = ParseMysql56GTIDSet(resultMap["Executed_Gtid_Set"])
	if err != nil {
		return PrimaryStatus{}, vterrors.Wrapf(err, "PrimaryStatus can't parse MySQL 5.6 GTID (Executed_Gtid_Set: %#v)", resultMap["Executed_Gtid_Set"])
	}

	return status, nil
}

// waitUntilPositionCommand is part of the Flavor interface.

// waitUntilPositionCommand is part of the Flavor interface.
func (mysqlFlavor) waitUntilPositionCommand(ctx context.Context, pos Position) (string, error) {
	// A timeout of 0 means wait indefinitely.
	timeoutSeconds := 0
	if deadline, ok := ctx.Deadline(); ok {
		timeout := time.Until(deadline)
		if timeout <= 0 {
			return "", vterrors.Errorf(vtrpc.Code_DEADLINE_EXCEEDED, "timed out waiting for position %v", pos)
		}

		// Only whole numbers of seconds are supported.
		timeoutSeconds = int(timeout.Seconds())
		if timeoutSeconds == 0 {
			// We don't want a timeout <1.0s to truncate down to become infinite.
			timeoutSeconds = 1
		}
	}

	return fmt.Sprintf("SELECT WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS('%s', %v)", pos, timeoutSeconds), nil
}

// readBinlogEvent is part of the Flavor interface.
func (mysqlFlavor) readBinlogEvent(c *Conn) (BinlogEvent, error) {
	result, err := c.ReadPacket()
	if err != nil {
		return nil, err
	}
	switch result[0] {
	case EOFPacket:
		return nil, NewSQLError(CRServerLost, SSUnknownSQLState, "%v", io.EOF)
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

// TablesWithSize56 is a query to select table along with size for mysql 5.6
const TablesWithSize56 = `SELECT table_name,
	table_type,
	UNIX_TIMESTAMP(create_time) AS uts_create_time,
	table_comment,
	SUM(data_length + index_length),
	SUM(data_length + index_length)
FROM information_schema.tables
WHERE table_schema = database()
GROUP BY table_name,
	table_type,
	uts_create_time,
	table_comment`

// TablesWithSize57 is a query to select table along with size for mysql 5.7.
//
// It's a little weird, because the JOIN predicate only works if the table and databases do not contain weird characters.
// If the join does not return any data, we fall back to the same fields as used in the mysql 5.6 query.
//
// We join with a subquery that materializes the data from `information_schema.innodb_sys_tablespaces`
// early for performance reasons. This effectively causes only a single read of `information_schema.innodb_sys_tablespaces`
// per query.
const TablesWithSize57 = `SELECT t.table_name,
	t.table_type,
	UNIX_TIMESTAMP(t.create_time),
	t.table_comment,
	IFNULL(SUM(i.file_size), SUM(t.data_length + t.index_length)),
	IFNULL(SUM(i.allocated_size), SUM(t.data_length + t.index_length))
FROM information_schema.tables t
LEFT OUTER JOIN (
	SELECT space, file_size, allocated_size, name
	FROM information_schema.innodb_sys_tablespaces
	WHERE name LIKE CONCAT(database(), '/%')
	GROUP BY space, file_size, allocated_size, name
) i ON i.name = CONCAT(t.table_schema, '/', t.table_name) or i.name LIKE CONCAT(t.table_schema, '/', t.table_name, '#p#%')
WHERE t.table_schema = database()
GROUP BY t.table_name, t.table_type, t.create_time, t.table_comment`

// TablesWithSize80 is a query to select table along with size for mysql 8.0
//
// We join with a subquery that materializes the data from `information_schema.innodb_sys_tablespaces`
// early for performance reasons. This effectively causes only a single read of `information_schema.innodb_tablespaces`
// per query.
// Note the following:
//   - We use UNION ALL to deal differently with partitioned tables vs. non-partitioned tables.
//     Originally, the query handled both, but that introduced "WHERE ... OR" conditions that led to poor query
//     optimization. By separating to UNION ALL we remove all "OR" conditions.
//   - We utilize `INFORMATION_SCHEMA`.`TABLES`.`CREATE_OPTIONS` column to do early pruning before the JOIN.
//   - `TABLES`.`TABLE_NAME` has `utf8mb4_0900_ai_ci` collation.  `INNODB_TABLESPACES`.`NAME` has `utf8mb3_general_ci`.
//     We normalize the collation to get better query performance (we force the casting at the time of our choosing)
//   - `create_options` is NULL for views, and therefore we need an additional UNION ALL to include views
const TablesWithSize80 = `SELECT t.table_name,
		t.table_type,
		UNIX_TIMESTAMP(t.create_time),
		t.table_comment,
		i.file_size,
		i.allocated_size
	FROM information_schema.tables t
		LEFT JOIN information_schema.innodb_tablespaces i
	ON i.name = CONCAT(t.table_schema, '/', t.table_name) COLLATE utf8_general_ci
	WHERE
		t.table_schema = database() AND not t.create_options <=> 'partitioned'
UNION ALL
	SELECT
		t.table_name,
		t.table_type,
		UNIX_TIMESTAMP(t.create_time),
		t.table_comment,
		SUM(i.file_size),
		SUM(i.allocated_size)
	FROM information_schema.tables t
		LEFT JOIN information_schema.innodb_tablespaces i
	ON i.name LIKE (CONCAT(t.table_schema, '/', t.table_name, '#p#%') COLLATE utf8_general_ci )
	WHERE
		t.table_schema = database() AND t.create_options <=> 'partitioned'
	GROUP BY
		t.table_schema, t.table_name, t.table_type, t.create_time, t.table_comment
`

// baseShowTablesWithSizes is part of the Flavor interface.
func (mysqlFlavor56) baseShowTablesWithSizes() string {
	return TablesWithSize56
}

// supportsCapability is part of the Flavor interface.
func (mysqlFlavor56) supportsCapability(serverVersion string, capability FlavorCapability) (bool, error) {
	switch capability {
	default:
		return false, nil
	}
}

// baseShowTablesWithSizes is part of the Flavor interface.
func (mysqlFlavor57) baseShowTablesWithSizes() string {
	return TablesWithSize57
}

// supportsCapability is part of the Flavor interface.
func (mysqlFlavor57) supportsCapability(serverVersion string, capability FlavorCapability) (bool, error) {
	switch capability {
	case MySQLJSONFlavorCapability:
		return true, nil
	default:
		return false, nil
	}
}

// baseShowTablesWithSizes is part of the Flavor interface.
func (mysqlFlavor80) baseShowTablesWithSizes() string {
	return TablesWithSize80
}

// supportsCapability is part of the Flavor interface.
func (mysqlFlavor80) supportsCapability(serverVersion string, capability FlavorCapability) (bool, error) {
	switch capability {
	case InstantDDLFlavorCapability,
		InstantExpandEnumCapability,
		InstantAddLastColumnFlavorCapability,
		InstantAddDropVirtualColumnFlavorCapability,
		InstantChangeColumnDefaultFlavorCapability:
		return true, nil
	case InstantAddDropColumnFlavorCapability:
		return ServerVersionAtLeast(serverVersion, 8, 0, 29)
	case TransactionalGtidExecutedFlavorCapability:
		return ServerVersionAtLeast(serverVersion, 8, 0, 17)
	case FastDropTableFlavorCapability:
		return ServerVersionAtLeast(serverVersion, 8, 0, 23)
	case MySQLJSONFlavorCapability:
		return true, nil
	case MySQLUpgradeInServerFlavorCapability:
		return ServerVersionAtLeast(serverVersion, 8, 0, 16)
	case DynamicRedoLogCapacityFlavorCapability:
		return ServerVersionAtLeast(serverVersion, 8, 0, 30)
	case DisableRedoLogFlavorCapability:
		return ServerVersionAtLeast(serverVersion, 8, 0, 21)
	default:
		return false, nil
	}
}

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
	"fmt"
	"io"
	"time"

	"context"

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

// masterGTIDSet is part of the Flavor interface.
func (mysqlFlavor) masterGTIDSet(c *Conn) (GTIDSet, error) {
	// keep @@global as lowercase, as some servers like the Ripple binlog server only honors a lowercase `global` value
	qr, err := c.ExecuteFetch("SELECT @@global.gtid_executed", 1, false)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "unexpected result format for gtid_executed: %#v", qr)
	}
	return parseMysql56GTIDSet(qr.Rows[0][0].ToString())
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

func (mysqlFlavor) stopReplicationCommand() string {
	return "STOP SLAVE"
}

func (mysqlFlavor) stopIOThreadCommand() string {
	return "STOP SLAVE IO_THREAD"
}

// sendBinlogDumpCommand is part of the Flavor interface.
func (mysqlFlavor) sendBinlogDumpCommand(c *Conn, serverID uint32, startPos Position) error {
	gtidSet, ok := startPos.GTIDSet.(Mysql56GTIDSet)
	if !ok {
		return vterrors.Errorf(vtrpc.Code_INTERNAL, "startPos.GTIDSet is wrong type - expected Mysql56GTIDSet, got: %#v", startPos.GTIDSet)
	}

	// Build the command.
	sidBlock := gtidSet.SIDBlock()
	return c.WriteComBinlogDumpGTID(serverID, "", 4, 0, sidBlock)
}

// resetReplicationCommands is part of the Flavor interface.
func (mysqlFlavor) resetReplicationCommands(c *Conn) []string {
	resetCommands := []string{
		"STOP SLAVE",
		"RESET SLAVE ALL", // "ALL" makes it forget master host:port.
		"RESET MASTER",    // This will also clear gtid_executed and gtid_purged.
	}
	if c.SemiSyncExtensionLoaded() {
		resetCommands = append(resetCommands, "SET GLOBAL rpl_semi_sync_master_enabled = false, GLOBAL rpl_semi_sync_slave_enabled = false") // semi-sync will be enabled if needed when replica is started.
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
func (mysqlFlavor) changeMasterArg() string {
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
			return ReplicationStatus{}, vterrors.Wrapf(err, "cannot decode MasterUUID")
		}
		status.MasterUUID = sid
	}

	var err error
	status.Position.GTIDSet, err = parseMysql56GTIDSet(resultMap["Executed_Gtid_Set"])
	if err != nil {
		return ReplicationStatus{}, vterrors.Wrapf(err, "ReplicationStatus can't parse MySQL 5.6 GTID (Executed_Gtid_Set: %#v)", resultMap["Executed_Gtid_Set"])
	}
	relayLogGTIDSet, err := parseMysql56GTIDSet(resultMap["Retrieved_Gtid_Set"])
	if err != nil {
		return ReplicationStatus{}, vterrors.Wrapf(err, "ReplicationStatus can't parse MySQL 5.6 GTID (Retrieved_Gtid_Set: %#v)", resultMap["Retrieved_Gtid_Set"])
	}
	// We take the union of the executed and retrieved gtidset, because the retrieved gtidset only represents GTIDs since
	// the relay log has been reset. To get the full Position, we need to take a union of executed GTIDSets, since these would
	// have been in the relay log's GTIDSet in the past, prior to a reset.
	status.RelayLogPosition.GTIDSet = status.Position.GTIDSet.Union(relayLogGTIDSet)

	return status, nil
}

// masterStatus is part of the Flavor interface.
func (mysqlFlavor) masterStatus(c *Conn) (MasterStatus, error) {
	qr, err := c.ExecuteFetch("SHOW MASTER STATUS", 100, true /* wantfields */)
	if err != nil {
		return MasterStatus{}, err
	}
	if len(qr.Rows) == 0 {
		// The query returned no data. We don't know how this could happen.
		return MasterStatus{}, ErrNoMasterStatus
	}

	resultMap, err := resultToMap(qr)
	if err != nil {
		return MasterStatus{}, err
	}

	return parseMysqlMasterStatus(resultMap)
}

func parseMysqlMasterStatus(resultMap map[string]string) (MasterStatus, error) {
	status := parseMasterStatus(resultMap)

	var err error
	status.Position.GTIDSet, err = parseMysql56GTIDSet(resultMap["Executed_Gtid_Set"])
	if err != nil {
		return MasterStatus{}, vterrors.Wrapf(err, "MasterStatus can't parse MySQL 5.6 GTID (Executed_Gtid_Set: %#v)", resultMap["Executed_Gtid_Set"])
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
	return NewMysql56BinlogEvent(result[1:]), nil
}

// enableBinlogPlaybackCommand is part of the Flavor interface.
func (mysqlFlavor) enableBinlogPlaybackCommand() string {
	return ""
}

// disableBinlogPlaybackCommand is part of the Flavor interface.
func (mysqlFlavor) disableBinlogPlaybackCommand() string {
	return ""
}

// TablesWithSize56 is a query to select table along with size for mysql 5.6
const TablesWithSize56 = `SELECT table_name, table_type, unix_timestamp(create_time), table_comment, SUM( data_length + index_length), SUM( data_length + index_length) 
		FROM information_schema.tables WHERE table_schema = database() group by table_name`

// TablesWithSize57 is a query to select table along with size for mysql 5.7.
// It's a little weird, because the JOIN predicate only works if the table and databases do not contain weird characters.
// As a fallback, we use the mysql 5.6 query, which is not always up to date, but works for all table/db names.
const TablesWithSize57 = `SELECT t.table_name, t.table_type, unix_timestamp(t.create_time), t.table_comment, i.file_size, i.allocated_size 
	FROM information_schema.tables t, information_schema.innodb_sys_tablespaces i 
	WHERE t.table_schema = database() and i.name = concat(t.table_schema,'/',t.table_name)
UNION ALL
	SELECT table_name, table_type, unix_timestamp(create_time), table_comment, SUM( data_length + index_length), SUM( data_length + index_length)
	FROM information_schema.tables t
	WHERE table_schema = database() AND NOT EXISTS(SELECT * FROM information_schema.innodb_sys_tablespaces i WHERE i.name = concat(t.table_schema,'/',t.table_name)) 
	group by table_name, table_type, unix_timestamp(create_time), table_comment
`

// TablesWithSize80 is a query to select table along with size for mysql 8.0
const TablesWithSize80 = `SELECT t.table_name, t.table_type, unix_timestamp(t.create_time), t.table_comment, i.file_size, i.allocated_size 
		FROM information_schema.tables t, information_schema.innodb_tablespaces i 
		WHERE t.table_schema = database() and i.name = concat(t.table_schema,'/',t.table_name)`

// baseShowTablesWithSizes is part of the Flavor interface.
func (mysqlFlavor56) baseShowTablesWithSizes() string {
	return TablesWithSize56
}

// baseShowTablesWithSizes is part of the Flavor interface.
func (mysqlFlavor57) baseShowTablesWithSizes() string {
	return TablesWithSize57
}

// baseShowTablesWithSizes is part of the Flavor interface.
func (mysqlFlavor80) baseShowTablesWithSizes() string {
	return TablesWithSize80
}

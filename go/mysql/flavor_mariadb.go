/*

Copyright 2017 Google Inc.

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
	"time"

	"golang.org/x/net/context"
)

// mariadbFlavor implements the Flavor interface for MariaDB.
type mariadbFlavor struct{}

// masterGTIDSet is part of the Flavor interface.
func (mariadbFlavor) masterGTIDSet(c *Conn) (GTIDSet, error) {
	qr, err := c.ExecuteFetch("SELECT @@GLOBAL.gtid_binlog_pos", 1, false)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
		return nil, fmt.Errorf("unexpected result format for gtid_binlog_pos: %#v", qr)
	}

	return parseMariadbGTIDSet(qr.Rows[0][0].ToString())
}

// sendBinlogDumpCommand is part of the Flavor interface.
func (mariadbFlavor) sendBinlogDumpCommand(c *Conn, slaveID uint32, startPos Position) error {
	// Tell the server that we understand GTIDs by setting our slave
	// capability to MARIA_SLAVE_CAPABILITY_GTID = 4 (MariaDB >= 10.0.1).
	if _, err := c.ExecuteFetch("SET @mariadb_slave_capability=4", 0, false); err != nil {
		return fmt.Errorf("failed to set @mariadb_slave_capability=4: %v", err)
	}

	// Set the slave_connect_state variable before issuing COM_BINLOG_DUMP
	// to provide the start position in GTID form.
	query := fmt.Sprintf("SET @slave_connect_state='%s'", startPos)
	if _, err := c.ExecuteFetch(query, 0, false); err != nil {
		return fmt.Errorf("failed to set @slave_connect_state='%s': %v", startPos, err)
	}

	// Real slaves set this upon connecting if their gtid_strict_mode option
	// was enabled. We always use gtid_strict_mode because we need it to
	// make our internal GTID comparisons safe.
	if _, err := c.ExecuteFetch("SET @slave_gtid_strict_mode=1", 0, false); err != nil {
		return fmt.Errorf("failed to set @slave_gtid_strict_mode=1: %v", err)
	}

	// Since we use @slave_connect_state, the file and position here are
	// ignored.
	return c.WriteComBinlogDump(slaveID, "", 0, 0)
}

// resetReplicationCommands is part of the Flavor interface.
func (mariadbFlavor) resetReplicationCommands() []string {
	return []string{
		"STOP SLAVE",
		"RESET SLAVE ALL", // "ALL" makes it forget master host:port.
		"RESET MASTER",
		"SET GLOBAL gtid_slave_pos = ''",
	}
}

// setSlavePositionCommands is part of the Flavor interface.
func (mariadbFlavor) setSlavePositionCommands(pos Position) []string {
	return []string{
		// RESET MASTER will clear out gtid_binlog_pos,
		// which then guarantees that gtid_current_pos = gtid_slave_pos,
		// since gtid_current_pos = MAX(gtid_binlog_pos,gtid_slave_pos).
		// This also emptys the binlogs, which allows us to set
		// gtid_binlog_state.
		"RESET MASTER",
		// Set gtid_slave_pos to tell the slave where to start
		// replicating.
		fmt.Sprintf("SET GLOBAL gtid_slave_pos = '%s'", pos),
		// Set gtid_binlog_state so that if this server later becomes a
		// master, it will know that it has seen everything up to and
		// including 'pos'. Otherwise, if another slave asks this
		// server to replicate starting at exactly 'pos', this server
		// will throw an error when in gtid_strict_mode, since it
		// doesn't see 'pos' in its binlog - it only has everything
		// AFTER.
		fmt.Sprintf("SET GLOBAL gtid_binlog_state = '%s'", pos),
	}
}

// setSlavePositionCommands is part of the Flavor interface.
func (mariadbFlavor) changeMasterArg() string {
	return "MASTER_USE_GTID = current_pos"
}

// status is part of the Flavor interface.
func (mariadbFlavor) status(c *Conn) (SlaveStatus, error) {
	qr, err := c.ExecuteFetch("SHOW ALL SLAVES STATUS", 100, true /* wantfields */)
	if err != nil {
		return SlaveStatus{}, err
	}
	if len(qr.Rows) == 0 {
		// The query returned no data, meaning the server
		// is not configured as a slave.
		return SlaveStatus{}, ErrNotSlave
	}

	resultMap, err := resultToMap(qr)
	if err != nil {
		return SlaveStatus{}, err
	}

	status := parseSlaveStatus(resultMap)
	status.Position.GTIDSet, err = parseMariadbGTIDSet(resultMap["Gtid_Slave_Pos"])
	if err != nil {
		return SlaveStatus{}, fmt.Errorf("SlaveStatus can't parse MariaDB GTID (Gtid_Slave_Pos: %#v): %v", resultMap["Gtid_Slave_Pos"], err)
	}
	return status, nil
}

// waitUntilPositionCommand is part of the Flavor interface.
//
// Note: Unlike MASTER_POS_WAIT(), MASTER_GTID_WAIT() will continue waiting even
// if the slave thread stops. If that is a problem, we'll have to change this.
func (mariadbFlavor) waitUntilPositionCommand(ctx context.Context, pos Position) (string, error) {
	if deadline, ok := ctx.Deadline(); ok {
		timeout := deadline.Sub(time.Now())
		if timeout <= 0 {
			return "", fmt.Errorf("timed out waiting for position %v", pos)
		}
		return fmt.Sprintf("SELECT MASTER_GTID_WAIT('%s', %.6f)", pos, timeout.Seconds()), nil
	}

	// Omit the timeout to wait indefinitely. In MariaDB, a timeout of 0 means
	// return immediately.
	return fmt.Sprintf("SELECT MASTER_GTID_WAIT('%s')", pos), nil
}

// makeBinlogEvent is part of the Flavor interface.
func (mariadbFlavor) makeBinlogEvent(buf []byte) BinlogEvent {
	return NewMariadbBinlogEvent(buf)
}

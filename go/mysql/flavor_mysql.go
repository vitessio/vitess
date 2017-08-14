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

// mysqlFlavor implements the Flavor interface for Mysql.
type mysqlFlavor struct{}

// masterGTIDSet is part of the Flavor interface.
func (mysqlFlavor) masterGTIDSet(c *Conn) (GTIDSet, error) {
	qr, err := c.ExecuteFetch("SELECT @@GLOBAL.gtid_executed", 1, false)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
		return nil, fmt.Errorf("unexpected result format for gtid_executed: %#v", qr)
	}
	return parseMysql56GTIDSet(qr.Rows[0][0].ToString())
}

// sendBinlogDumpCommand is part of the Flavor interface.
func (mysqlFlavor) sendBinlogDumpCommand(c *Conn, slaveID uint32, startPos Position) error {
	gtidSet, ok := startPos.GTIDSet.(Mysql56GTIDSet)
	if !ok {
		return fmt.Errorf("startPos.GTIDSet is wrong type - expected Mysql56GTIDSet, got: %#v", startPos.GTIDSet)
	}

	// Build the command.
	sidBlock := gtidSet.SIDBlock()
	return c.WriteComBinlogDumpGTID(slaveID, "", 4, 0, sidBlock)
}

// resetReplicationCommands is part of the Flavor interface.
func (mysqlFlavor) resetReplicationCommands() []string {
	return []string{
		"STOP SLAVE",
		"RESET SLAVE ALL", // "ALL" makes it forget master host:port.
		"RESET MASTER",    // This will also clear gtid_executed and gtid_purged.
	}
}

// setSlavePositionCommands is part of the Flavor interface.
func (mysqlFlavor) setSlavePositionCommands(pos Position) []string {
	return []string{
		"RESET MASTER", // We must clear gtid_executed before setting gtid_purged.
		fmt.Sprintf("SET GLOBAL gtid_purged = '%s'", pos),
	}
}

// setSlavePositionCommands is part of the Flavor interface.
func (mysqlFlavor) changeMasterArg() string {
	return "MASTER_AUTO_POSITION = 1"
}

// status is part of the Flavor interface.
func (mysqlFlavor) status(c *Conn) (SlaveStatus, error) {
	qr, err := c.ExecuteFetch("SHOW SLAVE STATUS", 100, true /* wantfields */)
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
	status.Position.GTIDSet, err = parseMysql56GTIDSet(resultMap["Executed_Gtid_Set"])
	if err != nil {
		return SlaveStatus{}, fmt.Errorf("SlaveStatus can't parse MySQL 5.6 GTID (Executed_Gtid_Set: %#v): %v", resultMap["Executed_Gtid_Set"], err)
	}
	return status, nil
}

// waitUntilPositionCommand is part of the Flavor interface.
func (mysqlFlavor) waitUntilPositionCommand(ctx context.Context, pos Position) (string, error) {
	// A timeout of 0 means wait indefinitely.
	timeoutSeconds := 0
	if deadline, ok := ctx.Deadline(); ok {
		timeout := deadline.Sub(time.Now())
		if timeout <= 0 {
			return "", fmt.Errorf("timed out waiting for position %v", pos)
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

// makeBinlogEvent is part of the Flavor interface.
func (mysqlFlavor) makeBinlogEvent(buf []byte) BinlogEvent {
	return NewMysql56BinlogEvent(buf)
}

// enableBinlogPlaybackCommand is part of the Flavor interface.
func (mysqlFlavor) enableBinlogPlaybackCommand() string {
	return ""
}

// disableBinlogPlaybackCommand is part of the Flavor interface.
func (mysqlFlavor) disableBinlogPlaybackCommand() string {
	return ""
}

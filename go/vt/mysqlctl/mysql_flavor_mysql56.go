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

package mysqlctl

import (
	"fmt"
	"strings"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysql"
)

// mysql56 is the implementation of MysqlFlavor for MySQL 5.6+.
type mysql56 struct {
}

const mysql56FlavorID = "MySQL56"

// VersionMatch implements MysqlFlavor.VersionMatch().
func (*mysql56) VersionMatch(version string) bool {
	return strings.HasPrefix(version, "5.6") ||
		strings.HasPrefix(version, "5.7") ||
		strings.HasPrefix(version, "8.0")
}

// SlaveStatus implements MysqlFlavor.SlaveStatus().
func (flavor *mysql56) SlaveStatus(mysqld *Mysqld) (Status, error) {
	fields, err := mysqld.fetchSuperQueryMap(context.TODO(), "SHOW SLAVE STATUS")
	if err != nil {
		return Status{}, err
	}
	if len(fields) == 0 {
		// The query returned no data, meaning the server
		// is not configured as a slave.
		return Status{}, ErrNotSlave
	}
	status := parseSlaveStatus(fields)

	status.Position, err = flavor.ParseReplicationPosition(fields["Executed_Gtid_Set"])
	if err != nil {
		return Status{}, fmt.Errorf("SlaveStatus can't parse MySQL 5.6 GTID (Executed_Gtid_Set: %#v): %v", fields["Executed_Gtid_Set"], err)
	}
	return status, nil
}

// WaitMasterPos implements MysqlFlavor.WaitMasterPos().
func (*mysql56) WaitMasterPos(ctx context.Context, mysqld *Mysqld, targetPos mysql.Position) error {
	var query string

	// A timeout of 0 means wait indefinitely.
	var timeoutSeconds int
	if deadline, ok := ctx.Deadline(); ok {
		timeout := deadline.Sub(time.Now())
		if timeout <= 0 {
			return fmt.Errorf("timed out waiting for position %v", targetPos)
		}
		// Only whole numbers of seconds are supported.
		timeoutSeconds = int(timeout.Seconds())
		if timeoutSeconds == 0 {
			// We don't want a timeout <1.0s to truncate down to become infinite.
			timeoutSeconds = 1
		}
	}

	query = fmt.Sprintf("SELECT WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS('%s', %v)", targetPos, timeoutSeconds)

	log.Infof("Waiting for minimum replication position with query: %v", query)
	qr, err := mysqld.FetchSuperQuery(ctx, query)
	if err != nil {
		return fmt.Errorf("WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS() failed: %v", err)
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
		return fmt.Errorf("unexpected result format from WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS(): %#v", qr)
	}
	result := qr.Rows[0][0]
	if result.IsNull() {
		return fmt.Errorf("WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS() failed: gtid_mode is OFF")
	}
	if result.String() == "-1" {
		return fmt.Errorf("timed out waiting for position %v", targetPos)
	}
	return nil
}

// SetSlavePositionCommands implements MysqlFlavor.
func (*mysql56) SetSlavePositionCommands(pos mysql.Position) ([]string, error) {
	return []string{
		"RESET MASTER", // We must clear gtid_executed before setting gtid_purged.
		fmt.Sprintf("SET GLOBAL gtid_purged = '%s'", pos),
	}, nil
}

// SetMasterCommands implements MysqlFlavor.SetMasterCommands().
func (*mysql56) SetMasterCommands(params *mysql.ConnParams, masterHost string, masterPort int, masterConnectRetry int) ([]string, error) {
	// Make CHANGE MASTER TO command.
	args := changeMasterArgs(params, masterHost, masterPort, masterConnectRetry)
	args = append(args, "MASTER_AUTO_POSITION = 1")
	changeMasterTo := "CHANGE MASTER TO\n  " + strings.Join(args, ",\n  ")

	return []string{changeMasterTo}, nil
}

// ParseGTID implements MysqlFlavor.ParseGTID().
func (*mysql56) ParseGTID(s string) (mysql.GTID, error) {
	return mysql.ParseGTID(mysql56FlavorID, s)
}

// ParseReplicationPosition implements MysqlFlavor.ParseReplicationPosition().
func (*mysql56) ParseReplicationPosition(s string) (mysql.Position, error) {
	return mysql.ParsePosition(mysql56FlavorID, s)
}

// MakeBinlogEvent implements MysqlFlavor.MakeBinlogEvent().
func (*mysql56) MakeBinlogEvent(buf []byte) mysql.BinlogEvent {
	return mysql.NewMysql56BinlogEvent(buf)
}

// EnableBinlogPlayback implements MysqlFlavor.EnableBinlogPlayback().
func (*mysql56) EnableBinlogPlayback(mysqld *Mysqld) error {
	return nil
}

// DisableBinlogPlayback implements MysqlFlavor.DisableBinlogPlayback().
func (*mysql56) DisableBinlogPlayback(mysqld *Mysqld) error {
	return nil
}

func init() {
	registerFlavorBuiltin(mysql56FlavorID, &mysql56{})
}

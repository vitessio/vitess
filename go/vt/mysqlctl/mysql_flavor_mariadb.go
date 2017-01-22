// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"fmt"
	"strings"
	"time"

	"golang.org/x/net/context"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysqlconn/replication"
	"github.com/youtube/vitess/go/sqldb"
)

// mariaDB10 is the implementation of MysqlFlavor for MariaDB 10.0.10
type mariaDB10 struct {
}

const mariadbFlavorID = "MariaDB"

// VersionMatch implements MysqlFlavor.VersionMatch().
func (*mariaDB10) VersionMatch(version string) bool {
	return strings.HasPrefix(version, "10.0") && strings.Contains(strings.ToLower(version), "mariadb")
}

// MasterPosition implements MysqlFlavor.MasterPosition().
func (flavor *mariaDB10) MasterPosition(mysqld *Mysqld) (rp replication.Position, err error) {
	qr, err := mysqld.FetchSuperQuery(context.TODO(), "SELECT @@GLOBAL.gtid_binlog_pos")
	if err != nil {
		return rp, err
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
		return rp, fmt.Errorf("unexpected result format for gtid_binlog_pos: %#v", qr)
	}
	return flavor.ParseReplicationPosition(qr.Rows[0][0].String())
}

// SlaveStatus implements MysqlFlavor.SlaveStatus().
func (flavor *mariaDB10) SlaveStatus(mysqld *Mysqld) (Status, error) {
	fields, err := mysqld.fetchSuperQueryMap(context.TODO(), "SHOW ALL SLAVES STATUS")
	if err != nil {
		return Status{}, err
	}
	if len(fields) == 0 {
		// The query returned no data, meaning the server
		// is not configured as a slave.
		return Status{}, ErrNotSlave
	}
	status := parseSlaveStatus(fields)

	status.Position, err = flavor.ParseReplicationPosition(fields["Gtid_Slave_Pos"])
	if err != nil {
		return Status{}, fmt.Errorf("SlaveStatus can't parse MariaDB GTID (Gtid_Slave_Pos: %#v): %v", fields["Gtid_Slave_Pos"], err)
	}
	return status, nil
}

// WaitMasterPos implements MysqlFlavor.WaitMasterPos().
//
// Note: Unlike MASTER_POS_WAIT(), MASTER_GTID_WAIT() will continue waiting even
// if the slave thread stops. If that is a problem, we'll have to change this.
func (*mariaDB10) WaitMasterPos(ctx context.Context, mysqld *Mysqld, targetPos replication.Position) error {
	var query string
	if deadline, ok := ctx.Deadline(); ok {
		timeout := deadline.Sub(time.Now())
		if timeout <= 0 {
			return fmt.Errorf("timed out waiting for position %v", targetPos)
		}
		query = fmt.Sprintf("SELECT MASTER_GTID_WAIT('%s', %.6f)", targetPos, timeout.Seconds())
	} else {
		// Omit the timeout to wait indefinitely. In MariaDB, a timeout of 0 means
		// return immediately.
		query = fmt.Sprintf("SELECT MASTER_GTID_WAIT('%s')", targetPos)
	}

	log.Infof("Waiting for minimum replication position with query: %v", query)
	qr, err := mysqld.FetchSuperQuery(ctx, query)
	if err != nil {
		return fmt.Errorf("MASTER_GTID_WAIT() failed: %v", err)
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
		return fmt.Errorf("unexpected result format from MASTER_GTID_WAIT(): %#v", qr)
	}
	result := qr.Rows[0][0].String()
	if result == "-1" {
		return fmt.Errorf("timed out waiting for position %v", targetPos)
	}
	return nil
}

// ResetReplicationCommands implements MysqlFlavor.ResetReplicationCommands().
func (*mariaDB10) ResetReplicationCommands() []string {
	return []string{
		"STOP SLAVE",
		"RESET SLAVE ALL", // "ALL" makes it forget the master host:port.
		"RESET MASTER",
		"SET GLOBAL gtid_slave_pos = ''",
	}
}

// PromoteSlaveCommands implements MysqlFlavor.PromoteSlaveCommands().
func (*mariaDB10) PromoteSlaveCommands() []string {
	return []string{
		"RESET SLAVE ALL", // "ALL" makes it forget the master host:port.
	}
}

// SetSlavePositionCommands implements MysqlFlavor.
func (*mariaDB10) SetSlavePositionCommands(pos replication.Position) ([]string, error) {
	return []string{
		// RESET MASTER will clear out gtid_binlog_pos,
		// which then guarantees that gtid_current_pos = gtid_slave_pos,
		// since gtid_current_pos = MAX(gtid_binlog_pos, gtid_slave_pos).
		// This also emptys the binlogs, which allows us to set gtid_binlog_state.
		"RESET MASTER",
		// Set gtid_slave_pos to tell the slave where to start replicating.
		fmt.Sprintf("SET GLOBAL gtid_slave_pos = '%s'", pos),
		// Set gtid_binlog_state so that if this server later becomes a master,
		// it will know that it has seen everything up to and including 'pos'.
		// Otherwise, if another slave asks this server to replicate starting at
		// exactly 'pos', this server will throw an error when in gtid_strict_mode,
		// since it doesn't see 'pos' in its binlog - it only has everything AFTER.
		fmt.Sprintf("SET GLOBAL gtid_binlog_state = '%s'", pos),
	}, nil
}

// SetMasterCommands implements MysqlFlavor.SetMasterCommands().
func (*mariaDB10) SetMasterCommands(params *sqldb.ConnParams, masterHost string, masterPort int, masterConnectRetry int) ([]string, error) {
	// Make CHANGE MASTER TO command.
	args := changeMasterArgs(params, masterHost, masterPort, masterConnectRetry)
	// MASTER_USE_GTID = current_pos means it will request binlogs starting at
	// MAX(master position, slave position), which handles the case where a
	// demoted master is being converted back into a slave. In that case, the
	// slave position might be behind the master position, since it stopped
	// updating when the server was promoted to master.
	args = append(args, "MASTER_USE_GTID = current_pos")
	changeMasterTo := "CHANGE MASTER TO\n  " + strings.Join(args, ",\n  ")

	return []string{changeMasterTo}, nil
}

// ParseGTID implements MysqlFlavor.ParseGTID().
func (*mariaDB10) ParseGTID(s string) (replication.GTID, error) {
	return replication.ParseGTID(mariadbFlavorID, s)
}

// ParseReplicationPosition implements MysqlFlavor.ParseReplicationposition().
func (*mariaDB10) ParseReplicationPosition(s string) (replication.Position, error) {
	return replication.ParsePosition(mariadbFlavorID, s)
}

// SendBinlogDumpCommand implements MysqlFlavor.SendBinlogDumpCommand().
func (*mariaDB10) SendBinlogDumpCommand(conn *SlaveConnection, startPos replication.Position) error {
	// Tell the server that we understand GTIDs by setting our slave capability
	// to MARIA_SLAVE_CAPABILITY_GTID = 4 (MariaDB >= 10.0.1).
	if _, err := conn.ExecuteFetch("SET @mariadb_slave_capability=4", 0, false); err != nil {
		return fmt.Errorf("failed to set @mariadb_slave_capability=4: %v", err)
	}

	// Set the slave_connect_state variable before issuing COM_BINLOG_DUMP
	// to provide the start position in GTID form.
	query := fmt.Sprintf("SET @slave_connect_state='%s'", startPos)
	if _, err := conn.ExecuteFetch(query, 0, false); err != nil {
		return fmt.Errorf("failed to set @slave_connect_state='%s': %v", startPos, err)
	}

	// Real slaves set this upon connecting if their gtid_strict_mode option was
	// enabled. We always use gtid_strict_mode because we need it to make our
	// internal GTID comparisons safe.
	if _, err := conn.ExecuteFetch("SET @slave_gtid_strict_mode=1", 0, false); err != nil {
		return fmt.Errorf("failed to set @slave_gtid_strict_mode=1: %v", err)
	}

	// Since we use @slave_connect_state, the file and position here are ignored.
	return conn.WriteComBinlogDump(conn.slaveID, "", 0, 0)
}

// MakeBinlogEvent implements MysqlFlavor.MakeBinlogEvent().
func (*mariaDB10) MakeBinlogEvent(buf []byte) replication.BinlogEvent {
	return replication.NewMariadbBinlogEvent(buf)
}

// EnableBinlogPlayback implements MysqlFlavor.EnableBinlogPlayback().
func (*mariaDB10) EnableBinlogPlayback(mysqld *Mysqld) error {
	return nil
}

// DisableBinlogPlayback implements MysqlFlavor.DisableBinlogPlayback().
func (*mariaDB10) DisableBinlogPlayback(mysqld *Mysqld) error {
	return nil
}

func init() {
	registerFlavorBuiltin(mariadbFlavorID, &mariaDB10{})
}

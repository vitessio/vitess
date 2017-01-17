// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"golang.org/x/net/context"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/vt/mysqlctl/replication"
)

// mysql56 is the implementation of MysqlFlavor for MySQL 5.6+.
type mysql56 struct {
}

const mysql56FlavorID = "MySQL56"

// VersionMatch implements MysqlFlavor.VersionMatch().
func (*mysql56) VersionMatch(version string) bool {
	return strings.HasPrefix(version, "5.6") || strings.HasPrefix(version, "5.7")
}

// MasterPosition implements MysqlFlavor.MasterPosition().
func (flavor *mysql56) MasterPosition(mysqld *Mysqld) (rp replication.Position, err error) {
	qr, err := mysqld.FetchSuperQuery(context.TODO(), "SELECT @@GLOBAL.gtid_executed")
	if err != nil {
		return rp, err
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
		return rp, fmt.Errorf("unexpected result format for gtid_executed: %#v", qr)
	}
	return flavor.ParseReplicationPosition(qr.Rows[0][0].String())
}

// SlaveStatus implements MysqlFlavor.SlaveStatus().
func (flavor *mysql56) SlaveStatus(mysqld *Mysqld) (replication.Status, error) {
	fields, err := mysqld.fetchSuperQueryMap(context.TODO(), "SHOW SLAVE STATUS")
	if err != nil {
		return replication.Status{}, err
	}
	if len(fields) == 0 {
		// The query returned no data, meaning the server
		// is not configured as a slave.
		return replication.Status{}, ErrNotSlave
	}
	status := parseSlaveStatus(fields)

	status.Position, err = flavor.ParseReplicationPosition(fields["Executed_Gtid_Set"])
	if err != nil {
		return replication.Status{}, fmt.Errorf("SlaveStatus can't parse MySQL 5.6 GTID (Executed_Gtid_Set: %#v): %v", fields["Executed_Gtid_Set"], err)
	}
	return status, nil
}

// WaitMasterPos implements MysqlFlavor.WaitMasterPos().
func (*mysql56) WaitMasterPos(ctx context.Context, mysqld *Mysqld, targetPos replication.Position) error {
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

// ResetReplicationCommands implements MysqlFlavor.ResetReplicationCommands().
func (*mysql56) ResetReplicationCommands() []string {
	return []string{
		"STOP SLAVE",
		"RESET SLAVE ALL", // "ALL" makes it forget the master host:port.
		"RESET MASTER",    // This will also clear gtid_executed and gtid_purged.
	}
}

// PromoteSlaveCommands implements MysqlFlavor.PromoteSlaveCommands().
func (*mysql56) PromoteSlaveCommands() []string {
	return []string{
		"RESET SLAVE ALL", // "ALL" makes it forget the master host:port.
	}
}

// SetSlavePositionCommands implements MysqlFlavor.
func (*mysql56) SetSlavePositionCommands(pos replication.Position) ([]string, error) {
	return []string{
		"RESET MASTER", // We must clear gtid_executed before setting gtid_purged.
		fmt.Sprintf("SET GLOBAL gtid_purged = '%s'", pos),
	}, nil
}

// SetMasterCommands implements MysqlFlavor.SetMasterCommands().
func (*mysql56) SetMasterCommands(params *sqldb.ConnParams, masterHost string, masterPort int, masterConnectRetry int) ([]string, error) {
	// Make CHANGE MASTER TO command.
	args := changeMasterArgs(params, masterHost, masterPort, masterConnectRetry)
	args = append(args, "MASTER_AUTO_POSITION = 1")
	changeMasterTo := "CHANGE MASTER TO\n  " + strings.Join(args, ",\n  ")

	return []string{changeMasterTo}, nil
}

// ParseGTID implements MysqlFlavor.ParseGTID().
func (*mysql56) ParseGTID(s string) (replication.GTID, error) {
	return replication.ParseGTID(mysql56FlavorID, s)
}

// ParseReplicationPosition implements MysqlFlavor.ParseReplicationPosition().
func (*mysql56) ParseReplicationPosition(s string) (replication.Position, error) {
	return replication.ParsePosition(mysql56FlavorID, s)
}

// SendBinlogDumpCommand implements MysqlFlavor.SendBinlogDumpCommand().
func (flavor *mysql56) SendBinlogDumpCommand(conn *SlaveConnection, startPos replication.Position) error {
	gtidSet, ok := startPos.GTIDSet.(replication.Mysql56GTIDSet)
	if !ok {
		return fmt.Errorf("startPos.GTIDSet is wrong type - expected Mysql56GTIDSet, got: %#v", startPos.GTIDSet)
	}

	// Build the command.
	sidBlock := gtidSet.SIDBlock()
	return conn.WriteComBinlogDumpGTID(conn.slaveID, "", 4, 0, sidBlock)
}

// MakeBinlogEvent implements MysqlFlavor.MakeBinlogEvent().
func (*mysql56) MakeBinlogEvent(buf []byte) replication.BinlogEvent {
	return NewMysql56BinlogEvent(buf)
}

// EnableBinlogPlayback implements MysqlFlavor.EnableBinlogPlayback().
func (*mysql56) EnableBinlogPlayback(mysqld *Mysqld) error {
	return nil
}

// DisableBinlogPlayback implements MysqlFlavor.DisableBinlogPlayback().
func (*mysql56) DisableBinlogPlayback(mysqld *Mysqld) error {
	return nil
}

// mysql56BinlogEvent wraps a raw packet buffer and provides methods to examine
// it by implementing replication.BinlogEvent. Some methods are pulled in from
// binlogEvent.
type mysql56BinlogEvent struct {
	binlogEvent
}

// NewMysql56BinlogEvent creates a BinlogEvent from given byte array
func NewMysql56BinlogEvent(buf []byte) replication.BinlogEvent {
	return mysql56BinlogEvent{binlogEvent: binlogEvent(buf)}
}

// IsGTID implements BinlogEvent.IsGTID().
func (ev mysql56BinlogEvent) IsGTID() bool {
	return ev.Type() == 33 // GTID_LOG_EVENT
}

// GTID implements BinlogEvent.GTID().
//
// Expected format:
//   # bytes   field
//   1         flags
//   16        SID (server UUID)
//   8         GNO (sequence number, signed int)
func (ev mysql56BinlogEvent) GTID(f replication.BinlogFormat) (replication.GTID, bool, error) {
	data := ev.Bytes()[f.HeaderLength:]
	var sid replication.SID
	copy(sid[:], data[1:1+16])
	gno := int64(binary.LittleEndian.Uint64(data[1+16 : 1+16+8]))
	return replication.Mysql56GTID{Server: sid, Sequence: gno}, false /* hasBegin */, nil
}

// PreviousGTIDs implements BinlogEvent.PreviousGTIDs().
func (ev mysql56BinlogEvent) PreviousGTIDs(f replication.BinlogFormat) (replication.Position, error) {
	data := ev.Bytes()[f.HeaderLength:]
	set, err := replication.NewMysql56GTIDSetFromSIDBlock(data)
	if err != nil {
		return replication.Position{}, err
	}
	return replication.Position{
		GTIDSet: set,
	}, nil
}

// StripChecksum implements BinlogEvent.StripChecksum().
func (ev mysql56BinlogEvent) StripChecksum(f replication.BinlogFormat) (replication.BinlogEvent, []byte, error) {
	switch f.ChecksumAlgorithm {
	case BinlogChecksumAlgOff, BinlogChecksumAlgUndef:
		// There is no checksum.
		return ev, nil, nil
	case BinlogChecksumAlgCRC32:
		// Checksum is the last 4 bytes of the event buffer.
		data := ev.Bytes()
		length := len(data)
		checksum := data[length-4:]
		data = data[:length-4]
		return mysql56BinlogEvent{binlogEvent: binlogEvent(data)}, checksum, nil
	default:
		// MySQL 5.6 does not guarantee that future checksum algorithms will be
		// 4 bytes, so we can't support them a priori.
		return ev, nil, fmt.Errorf("unsupported checksum algorithm: %v", f.ChecksumAlgorithm)
	}
}

func init() {
	registerFlavorBuiltin(mysql56FlavorID, &mysql56{})
}

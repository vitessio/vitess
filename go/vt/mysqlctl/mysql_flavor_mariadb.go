// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysql"
	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
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
func (flavor *mariaDB10) MasterPosition(mysqld *Mysqld) (rp proto.ReplicationPosition, err error) {
	qr, err := mysqld.fetchSuperQuery("SELECT @@GLOBAL.gtid_binlog_pos")
	if err != nil {
		return rp, err
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
		return rp, fmt.Errorf("unexpected result format for gtid_binlog_pos: %#v", qr)
	}
	return flavor.ParseReplicationPosition(qr.Rows[0][0].String())
}

// SlaveStatus implements MysqlFlavor.SlaveStatus().
func (flavor *mariaDB10) SlaveStatus(mysqld *Mysqld) (*proto.ReplicationStatus, error) {
	fields, err := mysqld.fetchSuperQueryMap("SHOW ALL SLAVES STATUS")
	if err != nil {
		return nil, ErrNotSlave
	}
	status := parseSlaveStatus(fields)

	status.Position, err = flavor.ParseReplicationPosition(fields["Gtid_Slave_Pos"])
	if err != nil {
		return nil, fmt.Errorf("SlaveStatus can't parse MariaDB GTID (Gtid_Slave_Pos: %#v): %v", fields["Gtid_Slave_Pos"], err)
	}
	return status, nil
}

// WaitMasterPos implements MysqlFlavor.WaitMasterPos().
//
// Note: Unlike MASTER_POS_WAIT(), MASTER_GTID_WAIT() will continue waiting even
// if the slave thread stops. If that is a problem, we'll have to change this.
func (*mariaDB10) WaitMasterPos(mysqld *Mysqld, targetPos proto.ReplicationPosition, waitTimeout time.Duration) error {
	var query string
	if waitTimeout == 0 {
		// Omit the timeout to wait indefinitely. In MariaDB, a timeout of 0 means
		// return immediately.
		query = fmt.Sprintf("SELECT MASTER_GTID_WAIT('%s')", targetPos)
	} else {
		query = fmt.Sprintf("SELECT MASTER_GTID_WAIT('%s', %.6f)", targetPos, waitTimeout.Seconds())
	}

	log.Infof("Waiting for minimum replication position with query: %v", query)
	qr, err := mysqld.fetchSuperQuery(query)
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

// PromoteSlaveCommands implements MysqlFlavor.PromoteSlaveCommands().
func (*mariaDB10) PromoteSlaveCommands() []string {
	return []string{
		"RESET SLAVE",
	}
}

// StartReplicationCommands implements MysqlFlavor.StartReplicationCommands().
func (*mariaDB10) StartReplicationCommands(params *mysql.ConnectionParams, status *proto.ReplicationStatus) ([]string, error) {
	// Make SET gtid_slave_pos command.
	setSlavePos := fmt.Sprintf("SET GLOBAL gtid_slave_pos = '%s'", status.Position)

	// Make CHANGE MASTER TO command.
	args := changeMasterArgs(params, status)
	args = append(args, "MASTER_USE_GTID = slave_pos")
	changeMasterTo := "CHANGE MASTER TO\n  " + strings.Join(args, ",\n  ")

	return []string{
		"STOP SLAVE",
		"RESET SLAVE",
		setSlavePos,
		changeMasterTo,
		"START SLAVE",
	}, nil
}

// ParseGTID implements MysqlFlavor.ParseGTID().
func (*mariaDB10) ParseGTID(s string) (proto.GTID, error) {
	return proto.ParseGTID(mariadbFlavorID, s)
}

// ParseReplicationPosition implements MysqlFlavor.ParseReplicationposition().
func (*mariaDB10) ParseReplicationPosition(s string) (proto.ReplicationPosition, error) {
	return proto.ParseReplicationPosition(mariadbFlavorID, s)
}

// SendBinlogDumpCommand implements MysqlFlavor.SendBinlogDumpCommand().
func (*mariaDB10) SendBinlogDumpCommand(mysqld *Mysqld, conn *SlaveConnection, startPos proto.ReplicationPosition) error {
	const COM_BINLOG_DUMP = 0x12

	// Tell the server that we understand GTIDs by setting our slave capability
	// to MARIA_SLAVE_CAPABILITY_GTID = 4 (MariaDB >= 10.0.1).
	if _, err := conn.ExecuteFetch("SET @mariadb_slave_capability=4", 0, false); err != nil {
		return fmt.Errorf("failed to set @mariadb_slave_capability=4: %v", err)
	}

	// Tell the server that we understand the format of events that will be used
	// if binlog_checksum is enabled on the server.
	if _, err := conn.ExecuteFetch("SET @master_binlog_checksum=@@global.binlog_checksum", 0, false); err != nil {
		return fmt.Errorf("failed to set @master_binlog_checksum=@@global.binlog_checksum: %v", err)
	}

	// Set the slave_connect_state variable before issuing COM_BINLOG_DUMP to
	// provide the start position in GTID form.
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
	buf := makeBinlogDumpCommand(0, 0, conn.slaveID, "")
	return conn.SendCommand(COM_BINLOG_DUMP, buf)
}

// MakeBinlogEvent implements MysqlFlavor.MakeBinlogEvent().
func (*mariaDB10) MakeBinlogEvent(buf []byte) blproto.BinlogEvent {
	return NewMariadbBinlogEvent(buf)
}

// EnableBinlogPlayback implements MysqlFlavor.EnableBinlogPlayback().
func (*mariaDB10) EnableBinlogPlayback(mysqld *Mysqld) error {
	return nil
}

// DisableBinlogPlayback implements MysqlFlavor.DisableBinlogPlayback().
func (*mariaDB10) DisableBinlogPlayback(mysqld *Mysqld) error {
	return nil
}

// mariadbBinlogEvent wraps a raw packet buffer and provides methods to examine
// it by implementing blproto.BinlogEvent. Some methods are pulled in from
// binlogEvent.
type mariadbBinlogEvent struct {
	binlogEvent
}

func NewMariadbBinlogEvent(buf []byte) blproto.BinlogEvent {
	return mariadbBinlogEvent{binlogEvent: binlogEvent(buf)}
}

// HasGTID implements BinlogEvent.HasGTID().
func (ev mariadbBinlogEvent) HasGTID(f blproto.BinlogFormat) bool {
	// MariaDB provides GTIDs in a separate event type GTID_EVENT.
	return ev.IsGTID()
}

// IsGTID implements BinlogEvent.IsGTID().
func (ev mariadbBinlogEvent) IsGTID() bool {
	return ev.Type() == 162
}

// IsBeginGTID implements BinlogEvent.IsBeginGTID().
//
// Expected format:
//   # bytes   field
//   8         sequence number
//   4         domain ID
//   1         flags2
func (ev mariadbBinlogEvent) IsBeginGTID(f blproto.BinlogFormat) bool {
	const FL_STANDALONE = 1

	data := ev.Bytes()[f.HeaderLength:]
	flags2 := data[8+4]
	return flags2&FL_STANDALONE == 0
}

// GTID implements BinlogEvent.GTID().
//
// Expected format:
//   # bytes   field
//   8         sequence number
//   4         domain ID
//   1         flags2
func (ev mariadbBinlogEvent) GTID(f blproto.BinlogFormat) (proto.GTID, error) {
	data := ev.Bytes()[f.HeaderLength:]

	return proto.MariadbGTID{
		Sequence: binary.LittleEndian.Uint64(data[:8]),
		Domain:   binary.LittleEndian.Uint32(data[8 : 8+4]),
		Server:   ev.ServerID(),
	}, nil
}

// Format overrides binlogEvent.Format().
func (ev mariadbBinlogEvent) Format() (f blproto.BinlogFormat, err error) {
	// Call parent.
	f, err = ev.binlogEvent.Format()
	if err != nil {
		return
	}

	// MariaDB 5.3+ always adds a 4-byte checksum to the end of a
	// FORMAT_DESCRIPTION_EVENT, regardless of the server setting. The byte
	// immediately before that checksum tells us which checksum algorithm (if any)
	// is used for the rest of the events.
	data := ev.Bytes()
	f.ChecksumAlgorithm = data[len(data)-5]
	return
}

// StripChecksum implements BinlogEvent.StripChecksum().
func (ev mariadbBinlogEvent) StripChecksum(f blproto.BinlogFormat) (blproto.BinlogEvent, []byte) {
	switch f.ChecksumAlgorithm {
	case BINLOG_CHECKSUM_ALG_OFF, BINLOG_CHECKSUM_ALG_UNDEF:
		// There is no checksum.
		return ev, nil
	default:
		// Checksum is the last 4 bytes of the event buffer.
		data := ev.Bytes()
		length := len(data)
		checksum := data[length-4:]
		data = data[:length-4]
		return mariadbBinlogEvent{binlogEvent: binlogEvent(data)}, checksum
	}
}

const (
	// BINLOG_CHECKSUM_ALG_OFF indicates that checksums are supported but off.
	BINLOG_CHECKSUM_ALG_OFF = 0
	// BINLOG_CHECKSUM_ALG_UNDEF indicates that checksums are not supported.
	BINLOG_CHECKSUM_ALG_UNDEF = 255
)

func init() {
	registerFlavorBuiltin(mariadbFlavorID, &mariaDB10{})
}

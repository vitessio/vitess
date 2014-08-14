// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"encoding/binary"
	"fmt"

	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

// googleMysql51 is the implementation of MysqlFlavor for google mysql 51
type googleMysql51 struct {
}

const googleMysqlFlavorID = "GoogleMysql"

// MasterStatus implements MysqlFlavor.MasterStatus
//
// The command looks like:
// mysql> show master status\G
// **************************** 1. row ***************************
// File: vt-000001c6-bin.000003
// Position: 106
// Binlog_Do_DB:
// Binlog_Ignore_DB:
// Group_ID:
func (flavor *googleMysql51) MasterStatus(mysqld *Mysqld) (rp *proto.ReplicationPosition, err error) {
	qr, err := mysqld.fetchSuperQuery("SHOW MASTER STATUS")
	if err != nil {
		return
	}
	if len(qr.Rows) != 1 {
		return nil, ErrNotMaster
	}
	if len(qr.Rows[0]) < 5 {
		return nil, fmt.Errorf("this db does not support group id")
	}
	rp = &proto.ReplicationPosition{}
	rp.MasterLogFile = qr.Rows[0][0].String()
	utemp, err := qr.Rows[0][1].ParseUint64()
	if err != nil {
		return nil, err
	}
	rp.MasterLogPosition = uint(utemp)
	rp.MasterLogGTIDField.Value, err = flavor.ParseGTID(qr.Rows[0][4].String())
	if err != nil {
		return nil, err
	}

	// On the master, the SQL position and IO position are at
	// necessarily the same point.
	rp.MasterLogFileIo = rp.MasterLogFile
	rp.MasterLogPositionIo = rp.MasterLogPosition
	return
}

// PromoteSlaveCommands implements MysqlFlavor.PromoteSlaveCommands
func (*googleMysql51) PromoteSlaveCommands() []string {
	return []string{
		"RESET MASTER",
		"RESET SLAVE",
		"CHANGE MASTER TO MASTER_HOST = ''",
	}
}

// ParseGTID implements MysqlFlavor.ParseGTID().
func (*googleMysql51) ParseGTID(s string) (proto.GTID, error) {
	return proto.ParseGTID(googleMysqlFlavorID, s)
}

// SendBinlogDumpCommand implements MysqlFlavor.SendBinlogDumpCommand().
func (*googleMysql51) SendBinlogDumpCommand(mysqld *Mysqld, conn *SlaveConnection, startPos proto.GTID) error {
	const COM_BINLOG_DUMP = 0x12

	// We can't use Google MySQL's group_id command COM_BINLOG_DUMP2 because it
	// requires us to know the server_id of the server that generated the event,
	// to avoid connecting to a master with an alternate future. We don't know
	// that server_id, so we have to use the old file and position method, which
	// bypasses that check.
	pos, err := mysqld.BinlogInfo(startPos)
	if err != nil {
		return fmt.Errorf("error computing start position: %v", err)
	}

	// Build the command.
	buf := makeBinlogDumpCommand(uint32(pos.MasterLogPosition), 0, conn.slaveID, pos.MasterLogFile)
	return conn.SendCommand(COM_BINLOG_DUMP, buf)
}

// MakeBinlogEvent implements MysqlFlavor.MakeBinlogEvent().
func (*googleMysql51) MakeBinlogEvent(buf []byte) blproto.BinlogEvent {
	return NewGoogleBinlogEvent(buf)
}

// googleBinlogEvent wraps a raw packet buffer and provides methods to examine
// it by implementing blproto.BinlogEvent. Some methods are pulled in from
// binlogEvent.
type googleBinlogEvent struct {
	binlogEvent
}

func NewGoogleBinlogEvent(buf []byte) blproto.BinlogEvent {
	return googleBinlogEvent{binlogEvent: binlogEvent(buf)}
}

// IsGTID implements BinlogEvent.IsGTID().
func (ev googleBinlogEvent) IsGTID() bool {
	// Google MySQL doesn't have a GTID_EVENT.
	return false
}

// Format implements BinlogEvent.Format().
func (ev googleBinlogEvent) Format() (blproto.BinlogFormat, error) {
	f, err := ev.binlogEvent.Format()
	if err != nil {
		return f, err
	}

	// Google MySQL extends the header length (normally 19 bytes) by 8 to add
	// the group_id to every event.
	if f.HeaderLength < 19+8 {
		return f, fmt.Errorf("Google MySQL header length = %v, should be >= %v", f.HeaderLength, 19+8)
	}
	return f, nil
}

// HasGTID implements BinlogEvent.HasGTID().
func (ev googleBinlogEvent) HasGTID(f blproto.BinlogFormat) bool {
	// The header is frozen for FORMAT_DESCRIPTION and ROTATE events, since they
	// occur before you know the header_length. Therefore, they don't have the
	// extended header with a group_id.
	if ev.IsFormatDescription() || ev.IsRotate() {
		return false
	}

	// The group_id field is in the header for every other event type, but it's 0
	// if that event type doesn't use it.
	return ev.groupID() > 0
}

// groupID returns the group_id from the Google MySQL event header.
func (ev googleBinlogEvent) groupID() uint64 {
	// Google extended the header (normally 19 bytes) to add an 8 byte group_id.
	return binary.LittleEndian.Uint64(ev.Bytes()[19 : 19+8])
}

// GTID implements BinlogEvent.GTID().
func (ev googleBinlogEvent) GTID(f blproto.BinlogFormat) (proto.GTID, error) {
	group_id := ev.groupID()
	if group_id == 0 {
		return nil, fmt.Errorf("invalid group_id 0")
	}
	return proto.GoogleGTID{GroupID: group_id}, nil
}

func init() {
	mysqlFlavors[googleMysqlFlavorID] = &googleMysql51{}
}

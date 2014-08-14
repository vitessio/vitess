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

// mariaDB10 is the implementation of MysqlFlavor for MariaDB 10.0.10
type mariaDB10 struct {
}

const mariadbFlavorID = "MariaDB"

// MasterStatus implements MysqlFlavor.MasterStatus
func (flavor *mariaDB10) MasterStatus(mysqld *Mysqld) (rp *proto.ReplicationPosition, err error) {
	// grab what we need from SHOW MASTER STATUS
	qr, err := mysqld.fetchSuperQuery("SHOW MASTER STATUS")
	if err != nil {
		return
	}
	if len(qr.Rows) != 1 {
		return nil, ErrNotMaster
	}
	if len(qr.Rows[0]) < 2 {
		return nil, fmt.Errorf("unknown format for SHOW MASTER STATUS")
	}
	rp = &proto.ReplicationPosition{}
	rp.MasterLogFile = qr.Rows[0][0].String()
	utemp, err := qr.Rows[0][1].ParseUint64()
	if err != nil {
		return nil, err
	}
	rp.MasterLogPosition = uint(utemp)

	// grab the corresponding GTID
	qr, err = mysqld.fetchSuperQuery(fmt.Sprintf("SELECT BINLOG_GTID_POS('%v', %v)", rp.MasterLogFile, rp.MasterLogPosition))
	if err != nil {
		return
	}
	if len(qr.Rows) != 1 {
		return nil, fmt.Errorf("BINLOG_GTID_POS failed with no rows")
	}
	if len(qr.Rows[0]) < 1 {
		return nil, fmt.Errorf("BINLOG_GTID_POS returned no result")
	}
	rp.MasterLogGTIDField.Value, err = flavor.ParseGTID(qr.Rows[0][0].String())
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
func (*mariaDB10) PromoteSlaveCommands() []string {
	return []string{
		"RESET SLAVE",
	}
}

// ParseGTID implements MysqlFlavor.ParseGTID().
func (*mariaDB10) ParseGTID(s string) (proto.GTID, error) {
	return proto.ParseGTID(mariadbFlavorID, s)
}

// SendBinlogDumpCommand implements MysqlFlavor.SendBinlogDumpCommand().
func (*mariaDB10) SendBinlogDumpCommand(mysqld *Mysqld, conn *SlaveConnection, startPos proto.GTID) error {
	const COM_BINLOG_DUMP = 0x12

	// MariaDB expects the slave to set the @slave_connect_state variable before
	// issuing COM_BINLOG_DUMP if it wants to use GTID mode.
	query := fmt.Sprintf("SET @slave_connect_state='%s'", startPos)
	_, err := conn.ExecuteFetch(query, 0, false)
	if err != nil {
		return fmt.Errorf("mariaDB10.SendBinlogDumpCommand: failed to set @slave_connect_state='%s': %v", startPos, err)
	}

	// Since we use @slave_connect_state, the file and position here are ignored.
	buf := makeBinlogDumpCommand(0, 0, conn.slaveID, "")
	return conn.SendCommand(COM_BINLOG_DUMP, buf)
}

// MakeBinlogEvent implements MysqlFlavor.MakeBinlogEvent().
func (*mariaDB10) MakeBinlogEvent(buf []byte) blproto.BinlogEvent {
	return NewMariadbBinlogEvent(buf)
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

// GTID implements BinlogEvent.GTID().
//
// Expected format (L = total length of event data):
//   # bytes   field
//   8         sequence number
//   4         domain ID
func (ev mariadbBinlogEvent) GTID(f blproto.BinlogFormat) (proto.GTID, error) {
	data := ev.Bytes()[f.HeaderLength:]

	return proto.MariadbGTID{
		Sequence: binary.LittleEndian.Uint64(data[:8]),
		Domain:   binary.LittleEndian.Uint32(data[8 : 8+4]),
		Server:   ev.ServerID(),
	}, nil
}

func init() {
	mysqlFlavors[mariadbFlavorID] = &mariaDB10{}
}

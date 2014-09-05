// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"encoding/binary"
	"fmt"
	"strconv"
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
	fields, err := mysqld.fetchSuperQueryMap("SHOW SLAVE STATUS")
	if err != nil {
		return nil, ErrNotSlave
	}
	status := &proto.ReplicationStatus{
		SlaveIORunning:  fields["Slave_IO_Running"] == "Yes",
		SlaveSQLRunning: fields["Slave_SQL_Running"] == "Yes",
	}
	status.Position, err = flavor.ParseReplicationPosition(fields["Exec_Master_Group_ID"])
	if err != nil {
		return nil, err
	}
	temp, _ := strconv.ParseUint(fields["Seconds_Behind_Master"], 10, 0)
	status.SecondsBehindMaster = uint(temp)
	return status, nil
}

// WaitMasterPos implements MysqlFlavor.WaitMasterPos().
//
// Note: Unlike MASTER_POS_WAIT(), MASTER_GTID_WAIT() will continue waiting even
// if the slave thread stops. If that is a problem, we'll have to change this.
func (*mariaDB10) WaitMasterPos(mysqld *Mysqld, targetPos proto.ReplicationPosition, waitTimeout time.Duration) error {
	query := fmt.Sprintf("SELECT MASTER_GTID_WAIT('%s', %.6f)", targetPos, waitTimeout.Seconds())

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

	// MariaDB expects the slave to set the @slave_connect_state variable before
	// issuing COM_BINLOG_DUMP if it wants to use GTID mode.
	query := fmt.Sprintf("SET @slave_connect_state='%s'", startPos)
	if _, err := conn.ExecuteFetch(query, 0, false); err != nil {
		return fmt.Errorf("failed to set @slave_connect_state='%s': %v", startPos, err)
	}

	// Real slaves send this upon connecting if their gtid_strict_mode option was
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

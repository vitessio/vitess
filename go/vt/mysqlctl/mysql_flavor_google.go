// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysql"
	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

// googleMysql51 is the implementation of MysqlFlavor for google mysql 51
type googleMysql51 struct {
}

const googleMysqlFlavorID = "GoogleMysql"

// VersionMatch implements MysqlFlavor.VersionMatch().
func (*googleMysql51) VersionMatch(version string) bool {
	return strings.HasPrefix(version, "5.1") && strings.Contains(strings.ToLower(version), "google")
}

// MasterPosition implements MysqlFlavor.MasterPosition().
//
// The command looks like:
// mysql> SHOW MASTER STATUS\G
// **************************** 1. row ***************************
// File: vt-000001c6-bin.000003
// Position: 106
// Binlog_Do_DB:
// Binlog_Ignore_DB:
// Group_ID:
func (flavor *googleMysql51) MasterPosition(mysqld *Mysqld) (rp proto.ReplicationPosition, err error) {
	fields, err := mysqld.fetchSuperQueryMap("SHOW MASTER STATUS")
	if err != nil {
		return rp, err
	}
	groupID, ok := fields["Group_ID"]
	if !ok {
		return rp, fmt.Errorf("this db does not support group id")
	}
	// Get the server_id that created this group_id.
	info, err := mysqld.fetchSuperQueryMap("SHOW BINLOG INFO FOR " + groupID)
	if err != nil {
		return proto.ReplicationPosition{}, err
	}
	// Google MySQL does not define a format to describe both a server_id and
	// group_id, so we invented one.
	pos := info["Server_ID"] + "-" + groupID
	return flavor.ParseReplicationPosition(pos)
}

// SlaveStatus implements MysqlFlavor.SlaveStatus().
func (flavor *googleMysql51) SlaveStatus(mysqld *Mysqld) (*proto.ReplicationStatus, error) {
	fields, err := mysqld.fetchSuperQueryMap("SHOW SLAVE STATUS")
	if err != nil {
		return nil, ErrNotSlave
	}
	status := parseSlaveStatus(fields)

	groupID := fields["Exec_Master_Group_ID"]

	// Get the server_id that created this group_id.
	info, err := mysqld.fetchSuperQueryMap("SHOW BINLOG INFO FOR " + groupID)
	if err != nil {
		return nil, fmt.Errorf("SlaveStatus can't get server_id for group_id (%v): %v", groupID, err)
	}
	// Create the fake Google GTID syntax we invented.
	pos := info["Server_ID"] + "-" + groupID
	status.Position, err = flavor.ParseReplicationPosition(pos)
	if err != nil {
		return nil, fmt.Errorf("SlaveStatus can't parse Google GTID (%v): %v", pos, err)
	}
	return status, nil
}

// WaitMasterPos implements MysqlFlavor.WaitMasterPos().
//
// waitTimeout of 0 means wait indefinitely.
//
// Google MySQL doesn't have a function to wait for a GTID. MASTER_POS_WAIT()
// requires a file:pos, which we don't know anymore because we're passing around
// only GTIDs internally now.
//
// We can't ask the local mysqld instance to convert the GTID with BinlogInfo()
// because this instance hasn't seen that GTID yet. For now, we have to poll.
//
// There used to be a function called Mysqld.WaitForMinimumReplicationPosition,
// which was the same as WaitMasterPos except it used polling because it worked
// on GTIDs. Now that WaitMasterPos uses GTIDs too, they've been merged.
func (*googleMysql51) WaitMasterPos(mysqld *Mysqld, targetPos proto.ReplicationPosition, waitTimeout time.Duration) error {
	stopTime := time.Now().Add(waitTimeout)
	for waitTimeout == 0 || time.Now().Before(stopTime) {
		status, err := mysqld.SlaveStatus()
		if err != nil {
			return err
		}

		if status.Position.AtLeast(targetPos) {
			return nil
		}

		if !status.SlaveRunning() && waitTimeout == 0 {
			return fmt.Errorf("slave not running during WaitMasterPos and no timeout is set, status = %+v", status)
		}

		log.Infof("WaitMasterPos got position %v, sleeping for 1s waiting for position %v", status.Position, targetPos)
		time.Sleep(time.Second)
	}
	return fmt.Errorf("timed out waiting for position %v", targetPos)
}

// PromoteSlaveCommands implements MysqlFlavor.PromoteSlaveCommands().
func (*googleMysql51) PromoteSlaveCommands() []string {
	return []string{
		"RESET MASTER",
		"RESET SLAVE",
		"CHANGE MASTER TO MASTER_HOST = ''",
	}
}

// StartReplicationCommands implements MysqlFlavor.StartReplicationCommands().
func (*googleMysql51) StartReplicationCommands(params *mysql.ConnectionParams, status *proto.ReplicationStatus) ([]string, error) {
	// Make SET binlog_group_id command. We have to cast to the Google-specific
	// struct to access the fields because there is no canonical printed format to
	// represent both a group_id and server_id in Google MySQL.
	gtid, ok := status.Position.GTIDSet.(proto.GoogleGTID)
	if !ok {
		return nil, fmt.Errorf("can't start replication at GTIDSet %#v, expected GoogleGTID", status.Position.GTIDSet)
	}
	setGroupID := fmt.Sprintf(
		"SET binlog_group_id = %d, master_server_id = %d",
		gtid.GroupID, gtid.ServerID)

	// Make CHANGE MASTER TO command.
	args := changeMasterArgs(params, status)
	args = append(args, "CONNECT_USING_GROUP_ID")
	changeMasterTo := "CHANGE MASTER TO\n  " + strings.Join(args, ",\n  ")

	return []string{
		"STOP SLAVE",
		"RESET SLAVE",
		setGroupID,
		changeMasterTo,
		"START SLAVE",
	}, nil
}

// ParseGTID implements MysqlFlavor.ParseGTID().
func (*googleMysql51) ParseGTID(s string) (proto.GTID, error) {
	return proto.ParseGTID(googleMysqlFlavorID, s)
}

// ParseReplicationPosition implements MysqlFlavor.ParseReplicationPosition().
func (*googleMysql51) ParseReplicationPosition(s string) (proto.ReplicationPosition, error) {
	return proto.ParseReplicationPosition(googleMysqlFlavorID, s)
}

// EnableBinlogPlayback implements MysqlFlavor.EnableBinlogPlayback().
func (*googleMysql51) EnableBinlogPlayback(mysqld *Mysqld) error {
	// The Google-specific option super_to_set_timestamp is on by default.
	// We need to turn it off when we're about to start binlog streamer.
	if err := mysqld.ExecuteSuperQuery("SET @@global.super_to_set_timestamp = 0"); err != nil {
		log.Errorf("Cannot set super_to_set_timestamp=0: %v", err)
		return fmt.Errorf("EnableBinlogPlayback: can't set super_to_timestamp=0: %v", err)
	}

	log.Info("Successfully set super_to_set_timestamp=0")
	return nil
}

// DisableBinlogPlayback implements MysqlFlavor.DisableBinlogPlayback().
func (*googleMysql51) DisableBinlogPlayback(mysqld *Mysqld) error {
	// Re-enable super_to_set_timestamp when we're done streaming.
	if err := mysqld.ExecuteSuperQuery("SET @@global.super_to_set_timestamp = 1"); err != nil {
		log.Warningf("Cannot set super_to_set_timestamp=1: %v", err)
		return fmt.Errorf("DisableBinlogPlayback: can't set super_to_timestamp=1: %v", err)
	}

	log.Info("Successfully set super_to_set_timestamp=1")
	return nil
}

// makeBinlogDump2Command builds a buffer containing the data for a Google MySQL
// COM_BINLOG_DUMP2 command.
func makeBinlogDump2Command(flags uint16, slave_id uint32, group_id uint64, source_server_id uint32) []byte {
	var buf bytes.Buffer
	buf.Grow(2 + 4 + 8 + 4)

	// binlog_flags (2 bytes)
	binary.Write(&buf, binary.LittleEndian, flags)
	// server_id of slave (4 bytes)
	binary.Write(&buf, binary.LittleEndian, slave_id)
	// group_id (8 bytes)
	binary.Write(&buf, binary.LittleEndian, group_id)
	// server_id of the server that generated the group_id (4 bytes)
	binary.Write(&buf, binary.LittleEndian, source_server_id)

	return buf.Bytes()
}

// SendBinlogDumpCommand implements MysqlFlavor.SendBinlogDumpCommand().
func (flavor *googleMysql51) SendBinlogDumpCommand(mysqld *Mysqld, conn *SlaveConnection, startPos proto.ReplicationPosition) error {
	const (
		COM_BINLOG_DUMP2    = 0x27
		BINLOG_USE_GROUP_ID = 0x04
	)

	gtid, ok := startPos.GTIDSet.(proto.GoogleGTID)
	if !ok {
		return fmt.Errorf("startPos.GTIDSet = %#v is wrong type, expected GoogleGTID", startPos.GTIDSet)
	}

	// Build the command.
	buf := makeBinlogDump2Command(BINLOG_USE_GROUP_ID, conn.slaveID, gtid.GroupID, gtid.ServerID)
	return conn.SendCommand(COM_BINLOG_DUMP2, buf)
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
	return proto.GoogleGTID{ServerID: ev.ServerID(), GroupID: group_id}, nil
}

func init() {
	registerFlavorBuiltin(googleMysqlFlavorID, &googleMysql51{})
}

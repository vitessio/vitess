// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"fmt"
	"time"

	"code.google.com/p/vitess/go/relog"
)

// if the master is still alive, then we need to demote it gracefully
// make it read-only, flush the writes and get the position
func (mysqld *Mysqld) DemoteMaster() (*ReplicationPosition, error) {
	// label as TYPE_REPLICA
	mysqld.SetReadOnly(true)
	cmds := []string{
		"FLUSH TABLES WITH READ LOCK",
		"UNLOCK TABLES",
	}
	if err := mysqld.executeSuperQueryList(cmds); err != nil {
		return nil, err
	}
	return mysqld.MasterStatus()
}

// setReadWrite: set the new master in read-write mode.
//
// replicationState: info slaves need to reparent themselves
// waitPosition: slaves can wait for this position when restarting replication
// timePromoted: this timestamp (unix nanoseconds) is inserted into _vt.replication_log to verify the replication config
func (mysqld *Mysqld) PromoteSlave(setReadWrite bool) (replicationState *ReplicationState, waitPosition *ReplicationPosition, timePromoted int64, err error) {
	if err = mysqld.StopSlave(); err != nil {
		return
	}

	// If we are forced, we have to get our status as a master, not a slave.
	lastRepPos, err := mysqld.SlaveStatus()
	if err == ErrNotSlave {
		lastRepPos, err = mysqld.MasterStatus()
	}
	if err != nil {
		return
	}

	cmds := []string{
		"RESET MASTER",
		"RESET SLAVE",
		"CHANGE MASTER TO MASTER_HOST = ''",
	}
	if err = mysqld.executeSuperQueryList(cmds); err != nil {
		return
	}
	replicationPosition, err := mysqld.MasterStatus()
	if err != nil {
		return
	}
	mysqldAddr := mysqld.IpAddr()
	replicationState, err = NewReplicationState(mysqldAddr)
	if err != nil {
		return
	}
	replicationState.ReplicationPosition = *replicationPosition
	lastPos := lastRepPos.MapKey()
	newAddr := replicationState.MasterAddr()
	newPos := replicationState.ReplicationPosition.MapKey()
	timePromoted = time.Now().UnixNano()
	// write a row to verify that replication is functioning
	cmds = []string{
		fmt.Sprintf("INSERT INTO _vt.replication_log (time_created_ns, note) VALUES (%v, 'reparent check')", timePromoted),
	}
	if err = mysqld.executeSuperQueryList(cmds); err != nil {
		return
	}
	// this is the wait-point for checking replication
	waitPosition, err = mysqld.MasterStatus()
	if err != nil {
		return
	}
	if waitPosition.MasterLogFile == replicationPosition.MasterLogFile && waitPosition.MasterLogPosition == replicationPosition.MasterLogPosition {
		// we inserted a row, but our binlog position didn't
		// change. This is a serious problem. we don't want to
		// ever promote a master like that.
		err = fmt.Errorf("cannot promote slave to master, non-functional binlogs")
		return
	}

	cmds = []string{
		fmt.Sprintf("INSERT INTO _vt.reparent_log (time_created_ns, last_position, new_addr, new_position, wait_position) VALUES (%v, '%v', '%v', '%v', '%v')", timePromoted, lastPos, newAddr, newPos, waitPosition.MapKey()),
	}
	if err = mysqld.executeSuperQueryList(cmds); err != nil {
		return
	}

	if setReadWrite {
		err = mysqld.SetReadOnly(false)
	}
	return
}

func (mysqld *Mysqld) RestartSlave(replicationState *ReplicationState, waitPosition *ReplicationPosition, timeCheck int64) error {
	relog.Info("Restart Slave")
	cmds, err := StartReplicationCommands(mysqld, replicationState)
	if err != nil {
		return err
	}
	if err := mysqld.executeSuperQueryList(cmds); err != nil {
		return err
	}

	if err := mysqld.WaitForSlaveStart(SlaveStartDeadline); err != nil {
		return err
	}

	if err := mysqld.WaitMasterPos(waitPosition, 0); err != nil {
		return err
	}

	return mysqld.CheckReplication(timeCheck)
}

// Check for the magic row inserted under controlled reparenting.
func (mysqld *Mysqld) CheckReplication(timeCheck int64) error {
	relog.Info("Check replication restarted")
	checkQuery := fmt.Sprintf("SELECT * FROM _vt.replication_log WHERE time_created_ns = %v",
		timeCheck)
	qr, err := mysqld.fetchSuperQuery(checkQuery)
	if err != nil {
		return err
	}
	if len(qr.Rows) != 1 {
		return fmt.Errorf("replication failed - unexpected row count %v", len(qr.Rows))
	}
	return nil
}

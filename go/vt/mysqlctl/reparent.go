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

/*
replicationState: info slaves need to reparent themselves
waitPosition: slaves can wait for this position when restarting replication
timePromoted: this timestamp (unix nanoseconds) is inserted into _vt.replication_test to verify the replication config
*/
func (mysqld *Mysqld) PromoteSlave() (replicationState *ReplicationState, waitPosition *ReplicationPosition, timePromoted int64, err error) {
	cmds := []string{
		"STOP SLAVE",
		"RESET MASTER",
		"RESET SLAVE",
	}
	if err = mysqld.executeSuperQueryList(cmds); err != nil {
		return
	}
	replicationPosition, err := mysqld.MasterStatus()
	if err != nil {
		return
	}
	replicationState = NewReplicationState(mysqld.Addr())
	replicationState.ReplicationPosition = *replicationPosition
	timePromoted = time.Now().UnixNano()
	// write a row to verify that replication is functioning
	cmd := fmt.Sprintf("INSERT INTO _vt.replication_test (time_created_ns) VALUES (%v)", timePromoted)
	if err = mysqld.executeSuperQuery(cmd); err != nil {
		return
	}
	// this is the wait-point for checking replication
	waitPosition, err = mysqld.MasterStatus()
	if err != nil {
		return
	}

	err = mysqld.SetReadOnly(false)
	return
}

func (mysqld *Mysqld) RestartSlave(replicationState *ReplicationState, waitPosition *ReplicationPosition, timeCheck int64) error {
	relog.Info("Restart Slave")
	cmds := []string{
		"STOP SLAVE",
		"RESET SLAVE",
	}
	cmds = append(cmds, StartReplicationCommands(replicationState)...)
	if err := mysqld.executeSuperQueryList(cmds); err != nil {
		return err
	}

	if err := mysqld.WaitForSlaveStart(SlaveStartDeadline); err != nil {
		return err
	}

	cmd := fmt.Sprintf("SELECT MASTER_POS_WAIT('%v', %v)",
		waitPosition.MasterLogFile, waitPosition.MasterLogPosition)
	if err := mysqld.executeSuperQuery(cmd); err != nil {
		return err
	}

	return mysqld.CheckReplication(timeCheck)
}

// Check for the magic row inserted under controlled reparenting.
func (mysqld *Mysqld) CheckReplication(timeCheck int64) error {
	relog.Info("Check Slave")
	checkQuery := fmt.Sprintf("SELECT * FROM _vt.replication_test WHERE time_created_ns = %v",
		timeCheck)
	rows, err := mysqld.fetchSuperQuery(checkQuery)
	if err != nil {
		return err
	}
	if len(rows) != 1 {
		return fmt.Errorf("replication failed - unexpected row count %v", len(rows))
	}
	return nil
}

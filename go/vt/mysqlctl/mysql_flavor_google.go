// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"fmt"

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
	rp.MasterLogGTID.GTID, err = flavor.ParseGTID(qr.Rows[0][4].String())
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

func init() {
	mysqlFlavors[googleMysqlFlavorID] = &googleMysql51{}
}

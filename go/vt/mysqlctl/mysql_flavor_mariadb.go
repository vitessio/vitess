// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

// mariaDB10 is the implementation of MysqlFlavor for MariaDB 10.0.10
type mariaDB10 struct {
}

// MasterStatus implements MysqlFlavor.MasterStatus
func (*mariaDB10) MasterStatus(mysqld *Mysqld) (rp *proto.ReplicationPosition, err error) {
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

	// grab the corresponding groupId
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
	gtid := qr.Rows[0][0].String()
	sgtid := strings.Split(gtid, "-")
	if len(sgtid) != 3 {
		return nil, fmt.Errorf("failed to split gtid in 3: '%v' '%v'", gtid, qr)
	}
	rp.MasterLogGroupId, err = strconv.ParseInt(sgtid[2], 10, 64)
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

func init() {
	mysqlFlavors["MariaDB"] = &mariaDB10{}
}

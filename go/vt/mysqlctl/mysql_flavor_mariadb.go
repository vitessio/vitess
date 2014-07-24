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

// ParseGTID implements MysqlFlavor.ParseGTID().
func (*mariaDB10) ParseGTID(s string) (GTID, error) {
	// Split into parts.
	parts := strings.Split(s, "-")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid MariaDB GTID (%v): expecting domain-server-sequence", s)
	}

	// Parse domain ID.
	domain, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid MariaDB GTID domain ID (%v): %v", parts[0], err)
	}

	// Parse server ID.
	server, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid MariaDB GTID server ID (%v): %v", parts[1], err)
	}

	// Parse sequence number.
	sequence, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid MariaDB GTID sequence number (%v): %v", parts[2], err)
	}

	return mariaGTID{
		domain:   uint32(domain),
		server:   uint32(server),
		sequence: sequence,
	}, nil
}

type mariaGTID struct {
	domain   uint32
	server   uint32
	sequence uint64
}

// String implements GTID.String().
func (gtid mariaGTID) String() string {
	return fmt.Sprintf("%d-%d-%d", gtid.domain, gtid.server, gtid.sequence)
}

// TryCompare implements GTID.TryCompare().
func (gtid mariaGTID) TryCompare(cmp GTID) (int, error) {
	other, ok := cmp.(mariaGTID)
	if !ok {
		return 0, fmt.Errorf("can't compare GTID, wrong type: %#v.TryCompare(%#v)",
			gtid, cmp)
	}

	if gtid.domain != other.domain {
		return 0, fmt.Errorf("can't compare GTID, MariaDB domain doesn't match: %v != %v", gtid.domain, other.domain)
	}
	if gtid.server != other.server {
		return 0, fmt.Errorf("can't compare GTID, MariaDB server doesn't match: %v != %v", gtid.server, other.server)
	}

	switch true {
	case gtid.sequence < other.sequence:
		return -1, nil
	case gtid.sequence > other.sequence:
		return 1, nil
	default:
		return 0, nil
	}
}

func init() {
	mysqlFlavors["MariaDB"] = &mariaDB10{}
}

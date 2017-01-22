// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"fmt"

	"github.com/youtube/vitess/go/mysqlconn/replication"
	"github.com/youtube/vitess/go/netutil"
)

// Status holds replication information from SHOW SLAVE STATUS.
type Status struct {
	Position            replication.Position
	SlaveIORunning      bool
	SlaveSQLRunning     bool
	SecondsBehindMaster uint
	MasterHost          string
	MasterPort          int
	MasterConnectRetry  int
}

// SlaveRunning returns true iff both the Slave IO and Slave SQL threads are
// running.
func (rs *Status) SlaveRunning() bool {
	return rs.SlaveIORunning && rs.SlaveSQLRunning
}

// MasterAddr returns the host:port address of the master.
func (rs *Status) MasterAddr() string {
	return netutil.JoinHostPort(rs.MasterHost, int32(rs.MasterPort))
}

// NewStatus creates a Status pointing to masterAddr.
func NewStatus(masterAddr string) (*Status, error) {
	host, port, err := netutil.SplitHostPort(masterAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid masterAddr: %q, %v", masterAddr, err)
	}
	return &Status{
		MasterConnectRetry: 10,
		MasterHost:         host,
		MasterPort:         port,
	}, nil
}

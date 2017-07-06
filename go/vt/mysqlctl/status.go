/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mysqlctl

import (
	"fmt"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/netutil"
)

// Status holds replication information from SHOW SLAVE STATUS.
type Status struct {
	Position            mysql.Position
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

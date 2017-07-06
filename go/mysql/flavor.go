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

package mysql

import (
	"fmt"
	"strings"
)

// flavor is the abstract interface for a flavor.
// Flavors are auto-detected upon connection using the server version.
// We have two major implementations (the main difference is the GTID
// handling):
// 1. Oracle MySQL 5.6, 5.7, 8.0, ...
// 2. MariaDB 10.X
type flavor interface {
	// masterGTIDSet returns the current GTIDSet of a server.
	masterGTIDSet(c *Conn) (GTIDSet, error)

	// sendBinlogDumpCommand sends the packet required to start
	// dumping binlogs from the specified location.
	sendBinlogDumpCommand(c *Conn, slaveID uint32, startPos Position) error

	// resetReplicationCommands returns the commands to completely reset
	// replication on the host.
	resetReplicationCommands() []string

	// setSlavePositionCommands returns the commands to set the
	// replication position at which the slave will resume.
	setSlavePositionCommands(pos Position) []string

	// changeMasterArg returns the specific parameter to add to
	// a change master command.
	changeMasterArg() string
}

// mariaDBReplicationHackPrefix is the prefix of a version for MariaDB 10.0
// versions, to work around replication bugs.
const mariaDBReplicationHackPrefix = "5.5.5-"

// mariaDBVersionString is present in
const mariaDBVersionString = "MariaDB"

// fillFlavor fills in c.Flavor based on c.ServerVersion.
// This is the same logic as the ConnectorJ java client. We try to recognize
// MariaDB as much as we can, but default to MySQL.
//
// MariaDB note: the server version returned here might look like:
// 5.5.5-10.0.21-MariaDB-...
// If that is the case, we are removing the 5.5.5- prefix.
// Note on such servers, 'select version()' would return 10.0.21-MariaDB-...
// as well (not matching what c.ServerVersion is, but matching after we remove
// the prefix).
func (c *Conn) fillFlavor() {
	if strings.HasPrefix(c.ServerVersion, mariaDBReplicationHackPrefix) {
		c.ServerVersion = c.ServerVersion[len(mariaDBReplicationHackPrefix):]
		c.flavor = mariadbFlavor{}
		return
	}

	if strings.Index(c.ServerVersion, mariaDBVersionString) != -1 {
		c.flavor = mariadbFlavor{}
		return
	}

	c.flavor = mysqlFlavor{}
}

//
// The following methods are dependent on the flavor.
// Only valid for client connections (will panic for server connections).
//

// MasterPosition returns the current master replication position.
func (c *Conn) MasterPosition() (Position, error) {
	gtidSet, err := c.flavor.masterGTIDSet(c)
	if err != nil {
		return Position{}, err
	}
	return Position{
		GTIDSet: gtidSet,
	}, nil
}

// SendBinlogDumpCommand sends the flavor-specific version of
// the COM_BINLOG_DUMP command to start dumping raw binlog
// events over a slave connection, starting at a given GTID.
func (c *Conn) SendBinlogDumpCommand(slaveID uint32, startPos Position) error {
	return c.flavor.sendBinlogDumpCommand(c, slaveID, startPos)
}

// ResetReplicationCommands returns the commands to completely reset
// replication on the host.
func (c *Conn) ResetReplicationCommands() []string {
	return c.flavor.resetReplicationCommands()
}

// SetSlavePositionCommands returns the commands to set the
// replication position at which the slave will resume
// when it is later reparented with SetMasterCommands.
func (c *Conn) SetSlavePositionCommands(pos Position) []string {
	return c.flavor.setSlavePositionCommands(pos)
}

// SetMasterCommand returns the command to use the provided master
// as the new master (without changing any GTID position).
// It is guaranteed to be called with replication stopped.
// It should not start or stop replication.
func (c *Conn) SetMasterCommand(params *ConnParams, masterHost string, masterPort int, masterConnectRetry int) string {
	args := []string{
		fmt.Sprintf("MASTER_HOST = '%s'", masterHost),
		fmt.Sprintf("MASTER_PORT = %d", masterPort),
		fmt.Sprintf("MASTER_USER = '%s'", params.Uname),
		fmt.Sprintf("MASTER_PASSWORD = '%s'", params.Pass),
		fmt.Sprintf("MASTER_CONNECT_RETRY = %d", masterConnectRetry),
	}
	if params.SslEnabled() {
		args = append(args, "MASTER_SSL = 1")
	}
	if params.SslCa != "" {
		args = append(args, fmt.Sprintf("MASTER_SSL_CA = '%s'", params.SslCa))
	}
	if params.SslCaPath != "" {
		args = append(args, fmt.Sprintf("MASTER_SSL_CAPATH = '%s'", params.SslCaPath))
	}
	if params.SslCert != "" {
		args = append(args, fmt.Sprintf("MASTER_SSL_CERT = '%s'", params.SslCert))
	}
	if params.SslKey != "" {
		args = append(args, fmt.Sprintf("MASTER_SSL_KEY = '%s'", params.SslKey))
	}
	args = append(args, c.flavor.changeMasterArg())
	return "CHANGE MASTER TO\n  " + strings.Join(args, ",\n  ")
}

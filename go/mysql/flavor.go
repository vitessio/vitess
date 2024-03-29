/*
Copyright 2019 The Vitess Authors.

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
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"vitess.io/vitess/go/mysql/capabilities"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	// ErrNotReplica means there is no replication status.
	// Returned by ShowReplicationStatus().
	ErrNotReplica = sqlerror.NewSQLError(sqlerror.ERNotReplica, sqlerror.SSUnknownSQLState, "no replication status")

	// ErrNoPrimaryStatus means no status was returned by ShowPrimaryStatus().
	ErrNoPrimaryStatus = errors.New("no master status")
)

const (
	// mariaDBReplicationHackPrefix is the prefix of a version for MariaDB 10.0
	// versions, to work around replication bugs.
	mariaDBReplicationHackPrefix = "5.5.5-"
	// mariaDBVersionString is present in
	mariaDBVersionString = "MariaDB"
	// mysql57VersionPrefix is the prefix for 5.7 mysql version, such as 5.7.31-log
	mysql57VersionPrefix = "5.7."
	// mysql80VersionPrefix is the prefix for 8.0 mysql version, such as 8.0.19
	mysql80VersionPrefix = "8.0."
)

// flavor is the abstract interface for a flavor.
// Flavors are auto-detected upon connection using the server version.
// We have two major implementations (the main difference is the GTID
// handling):
// 1. Oracle MySQL 5.6, 5.7, 8.0, ...
// 2. MariaDB 10.X
type flavor interface {
	// primaryGTIDSet returns the current GTIDSet of a server.
	primaryGTIDSet(c *Conn) (replication.GTIDSet, error)

	// purgedGTIDSet returns the purged GTIDSet of a server.
	purgedGTIDSet(c *Conn) (replication.GTIDSet, error)

	// gtidMode returns the gtid mode of a server.
	gtidMode(c *Conn) (string, error)

	// serverUUID returns the UUID of a server.
	serverUUID(c *Conn) (string, error)

	// startReplicationCommand returns the command to start the replication.
	startReplicationCommand() string

	// restartReplicationCommands returns the commands to stop, reset and start the replication.
	restartReplicationCommands() []string

	// startReplicationUntilAfter will start replication, but only allow it
	// to run until `pos` is reached. After reaching pos, replication will be stopped again
	startReplicationUntilAfter(pos replication.Position) string

	// startSQLThreadUntilAfter will start replication's sql thread(s), but only allow it
	// to run until `pos` is reached. After reaching pos, it will be stopped again
	startSQLThreadUntilAfter(pos replication.Position) string

	// stopReplicationCommand returns the command to stop the replication.
	stopReplicationCommand() string

	// stopIOThreadCommand returns the command to stop the replica's IO thread only.
	stopIOThreadCommand() string

	// stopSQLThreadCommand returns the command to stop the replica's SQL thread(s) only.
	stopSQLThreadCommand() string

	// startSQLThreadCommand returns the command to start the replica's SQL thread only.
	startSQLThreadCommand() string

	// sendBinlogDumpCommand sends the packet required to start
	// dumping binlogs from the specified location.
	sendBinlogDumpCommand(c *Conn, serverID uint32, binlogFilename string, startPos replication.Position) error

	// readBinlogEvent reads the next BinlogEvent from the connection.
	readBinlogEvent(c *Conn) (BinlogEvent, error)

	// resetReplicationCommands returns the commands to completely reset
	// replication on the host.
	resetReplicationCommands(c *Conn) []string

	// resetReplicationParametersCommands returns the commands to reset
	// replication parameters on the host.
	resetReplicationParametersCommands(c *Conn) []string

	// setReplicationPositionCommands returns the commands to set the
	// replication position at which the replica will resume.
	setReplicationPositionCommands(pos replication.Position) []string

	// changeReplicationSourceArg returns the specific parameter to add to
	// a "change primary" command.
	changeReplicationSourceArg() string

	// status returns the result of the appropriate status command,
	// with parsed replication position.
	status(c *Conn) (replication.ReplicationStatus, error)

	// primaryStatus returns the result of 'SHOW MASTER STATUS',
	// with parsed executed position.
	primaryStatus(c *Conn) (replication.PrimaryStatus, error)

	// waitUntilPosition waits until the given position is reached or
	// until the context expires. It returns an error if we did not
	// succeed.
	waitUntilPosition(ctx context.Context, c *Conn, pos replication.Position) error

	baseShowTables() string
	baseShowTablesWithSizes() string

	supportsCapability(capability capabilities.FlavorCapability) (bool, error)
}

// flavorFuncs maps flavor names to their implementation.
// Flavors need to register only if they support being specified in the
// connection parameters.
var flavorFuncs = make(map[string]func() flavor)

// GetFlavor fills in c.Flavor. If the params specify the flavor,
// that is used. Otherwise, we auto-detect.
//
// This is the same logic as the ConnectorJ java client. We try to recognize
// MariaDB as much as we can, but default to MySQL.
//
// MariaDB note: the server version returned here might look like:
// 5.5.5-10.0.21-MariaDB-...
// If that is the case, we are removing the 5.5.5- prefix.
// Note on such servers, 'select version()' would return 10.0.21-MariaDB-...
// as well (not matching what c.ServerVersion is, but matching after we remove
// the prefix).
func GetFlavor(serverVersion string, flavorFunc func() flavor) (f flavor, capableOf capabilities.CapableOf, canonicalVersion string) {
	canonicalVersion = serverVersion
	switch {
	case flavorFunc != nil:
		f = flavorFunc()
	case strings.HasPrefix(serverVersion, mariaDBReplicationHackPrefix):
		canonicalVersion = serverVersion[len(mariaDBReplicationHackPrefix):]
		f = mariadbFlavor101{mariadbFlavor{serverVersion: canonicalVersion}}
	case strings.Contains(serverVersion, mariaDBVersionString):
		mariadbVersion, err := strconv.ParseFloat(serverVersion[:4], 64)
		if err != nil || mariadbVersion < 10.2 {
			f = mariadbFlavor101{mariadbFlavor{serverVersion: fmt.Sprintf("%f", mariadbVersion)}}
		} else {
			f = mariadbFlavor102{mariadbFlavor{serverVersion: fmt.Sprintf("%f", mariadbVersion)}}
		}
	case strings.HasPrefix(serverVersion, mysql57VersionPrefix):
		f = mysqlFlavor57{mysqlFlavor{serverVersion: serverVersion}}
	case strings.HasPrefix(serverVersion, mysql80VersionPrefix):
		f = mysqlFlavor80{mysqlFlavor{serverVersion: serverVersion}}
	default:
		// If unknown, return the most basic flavor: MySQL 56.
		f = mysqlFlavor56{mysqlFlavor{serverVersion: serverVersion}}
	}
	return f, f.supportsCapability, canonicalVersion
}

// ServerVersionCapableOf is a convenience function that returns a CapableOf function given a server version.
// It is a shortcut for GetFlavor(serverVersion, nil).
func ServerVersionCapableOf(serverVersion string) (capableOf capabilities.CapableOf) {
	_, capableOf, _ = GetFlavor(serverVersion, nil)
	return capableOf
}

// fillFlavor fills in c.Flavor. If the params specify the flavor,
// that is used. Otherwise, we auto-detect.
//
// This is the same logic as the ConnectorJ java client. We try to recognize
// MariaDB as much as we can, but default to MySQL.
//
// MariaDB note: the server version returned here might look like:
// 5.5.5-10.0.21-MariaDB-...
// If that is the case, we are removing the 5.5.5- prefix.
// Note on such servers, 'select version()' would return 10.0.21-MariaDB-...
// as well (not matching what c.ServerVersion is, but matching after we remove
// the prefix).
func (c *Conn) fillFlavor(params *ConnParams) {
	flavorFunc := flavorFuncs[params.Flavor]
	c.flavor, _, c.ServerVersion = GetFlavor(c.ServerVersion, flavorFunc)
}

// ServerVersionAtLeast returns 'true' if server version is equal or greater than given parts. e.g.
// "8.0.14-log" is at least [8, 0, 13] and [8, 0, 14], but not [8, 0, 15]
func (c *Conn) ServerVersionAtLeast(parts ...int) (bool, error) {
	return capabilities.ServerVersionAtLeast(c.ServerVersion, parts...)
}

//
// The following methods are dependent on the flavor.
// Only valid for client connections (will panic for server connections).
//

// IsMariaDB returns true iff the other side of the client connection
// is identified as MariaDB. Most applications should not care, but
// this is useful in tests.
func (c *Conn) IsMariaDB() bool {
	switch c.flavor.(type) {
	case mariadbFlavor101, mariadbFlavor102:
		return true
	}
	return false
}

// PrimaryPosition returns the current primary's replication position.
func (c *Conn) PrimaryPosition() (replication.Position, error) {
	gtidSet, err := c.flavor.primaryGTIDSet(c)
	if err != nil {
		return replication.Position{}, err
	}
	return replication.Position{
		GTIDSet: gtidSet,
	}, nil
}

// GetGTIDPurged returns the tablet's GTIDs which are purged.
func (c *Conn) GetGTIDPurged() (replication.Position, error) {
	gtidSet, err := c.flavor.purgedGTIDSet(c)
	if err != nil {
		return replication.Position{}, err
	}
	return replication.Position{
		GTIDSet: gtidSet,
	}, nil
}

// GetGTIDMode returns the tablet's GTID mode. Only available in MySQL flavour
func (c *Conn) GetGTIDMode() (string, error) {
	return c.flavor.gtidMode(c)
}

// GetServerUUID returns the server's UUID.
func (c *Conn) GetServerUUID() (string, error) {
	return c.flavor.serverUUID(c)
}

// PrimaryFilePosition returns the current primary's file based replication position.
func (c *Conn) PrimaryFilePosition() (replication.Position, error) {
	filePosFlavor := filePosFlavor{}
	gtidSet, err := filePosFlavor.primaryGTIDSet(c)
	if err != nil {
		return replication.Position{}, err
	}
	return replication.Position{
		GTIDSet: gtidSet,
	}, nil
}

// StartReplicationCommand returns the command to start replication.
func (c *Conn) StartReplicationCommand() string {
	return c.flavor.startReplicationCommand()
}

// RestartReplicationCommands returns the commands to stop, reset and start replication.
func (c *Conn) RestartReplicationCommands() []string {
	return c.flavor.restartReplicationCommands()
}

// StartReplicationUntilAfterCommand returns the command to start replication.
func (c *Conn) StartReplicationUntilAfterCommand(pos replication.Position) string {
	return c.flavor.startReplicationUntilAfter(pos)
}

// StartSQLThreadUntilAfterCommand returns the command to start the replica's SQL
// thread(s) and have it run until it has reached the given position, at which point
// it will stop.
func (c *Conn) StartSQLThreadUntilAfterCommand(pos replication.Position) string {
	return c.flavor.startSQLThreadUntilAfter(pos)
}

// StopReplicationCommand returns the command to stop the replication.
func (c *Conn) StopReplicationCommand() string {
	return c.flavor.stopReplicationCommand()
}

// StopIOThreadCommand returns the command to stop the replica's io thread.
func (c *Conn) StopIOThreadCommand() string {
	return c.flavor.stopIOThreadCommand()
}

// StopSQLThreadCommand returns the command to stop the replica's SQL thread(s).
func (c *Conn) StopSQLThreadCommand() string {
	return c.flavor.stopSQLThreadCommand()
}

// StartSQLThreadCommand returns the command to start the replica's SQL thread.
func (c *Conn) StartSQLThreadCommand() string {
	return c.flavor.startSQLThreadCommand()
}

// SendBinlogDumpCommand sends the flavor-specific version of
// the COM_BINLOG_DUMP command to start dumping raw binlog
// events over a server connection, starting at a given GTID.
func (c *Conn) SendBinlogDumpCommand(serverID uint32, binlogFilename string, startPos replication.Position) error {
	return c.flavor.sendBinlogDumpCommand(c, serverID, binlogFilename, startPos)
}

// ReadBinlogEvent reads the next BinlogEvent. This must be used
// in conjunction with SendBinlogDumpCommand.
func (c *Conn) ReadBinlogEvent() (BinlogEvent, error) {
	return c.flavor.readBinlogEvent(c)
}

// ResetReplicationCommands returns the commands to completely reset
// replication on the host.
func (c *Conn) ResetReplicationCommands() []string {
	return c.flavor.resetReplicationCommands(c)
}

// ResetReplicationParametersCommands returns the commands to reset
// replication parameters on the host.
func (c *Conn) ResetReplicationParametersCommands() []string {
	return c.flavor.resetReplicationParametersCommands(c)
}

// SetReplicationPositionCommands returns the commands to set the
// replication position at which the replica will resume
// when it is later reparented with SetReplicationSourceCommand.
func (c *Conn) SetReplicationPositionCommands(pos replication.Position) []string {
	return c.flavor.setReplicationPositionCommands(pos)
}

// SetReplicationSourceCommand returns the command to use the provided host/port
// as the new replication source (without changing any GTID position).
// It is guaranteed to be called with replication stopped.
// It should not start or stop replication.
func (c *Conn) SetReplicationSourceCommand(params *ConnParams, host string, port int32, connectRetry int) string {
	args := []string{
		fmt.Sprintf("MASTER_HOST = '%s'", host),
		fmt.Sprintf("MASTER_PORT = %d", port),
		fmt.Sprintf("MASTER_USER = '%s'", params.Uname),
		fmt.Sprintf("MASTER_PASSWORD = '%s'", params.Pass),
		fmt.Sprintf("MASTER_CONNECT_RETRY = %d", connectRetry),
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
	args = append(args, c.flavor.changeReplicationSourceArg())
	return "CHANGE MASTER TO\n  " + strings.Join(args, ",\n  ")
}

// resultToMap is a helper function used by ShowReplicationStatus.
func resultToMap(qr *sqltypes.Result) (map[string]string, error) {
	if len(qr.Rows) == 0 {
		// The query succeeded, but there is no data.
		return nil, nil
	}
	if len(qr.Rows) > 1 {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "query returned %d rows, expected 1", len(qr.Rows))
	}
	if len(qr.Fields) != len(qr.Rows[0]) {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "query returned %d column names, expected %d", len(qr.Fields), len(qr.Rows[0]))
	}

	result := make(map[string]string, len(qr.Fields))
	for i, field := range qr.Fields {
		result[field.Name] = qr.Rows[0][i].ToString()
	}
	return result, nil
}

// ShowReplicationStatus executes the right command to fetch replication status,
// and returns a parsed Position with other fields.
func (c *Conn) ShowReplicationStatus() (replication.ReplicationStatus, error) {
	return c.flavor.status(c)
}

// ShowPrimaryStatus executes the right SHOW MASTER STATUS command,
// and returns a parsed executed Position, as well as file based Position.
func (c *Conn) ShowPrimaryStatus() (replication.PrimaryStatus, error) {
	return c.flavor.primaryStatus(c)
}

// WaitUntilPosition waits until the given position is reached or until the
// context expires. It returns an error if we did not succeed.
func (c *Conn) WaitUntilPosition(ctx context.Context, pos replication.Position) error {
	return c.flavor.waitUntilPosition(ctx, c, pos)
}

// WaitUntilFilePosition waits until the given position is reached or until
// the context expires for the file position flavor. It returns an error if
// we did not succeed.
func (c *Conn) WaitUntilFilePosition(ctx context.Context, pos replication.Position) error {
	filePosFlavor := filePosFlavor{}
	return filePosFlavor.waitUntilPosition(ctx, c, pos)
}

// BaseShowTables returns a query that shows tables
func (c *Conn) BaseShowTables() string {
	return c.flavor.baseShowTables()
}

// BaseShowTablesWithSizes returns a query that shows tables and their sizes
func (c *Conn) BaseShowTablesWithSizes() string {
	return c.flavor.baseShowTablesWithSizes()
}

// SupportsCapability checks if the database server supports the given capability
func (c *Conn) SupportsCapability(capability capabilities.FlavorCapability) (bool, error) {
	return c.flavor.supportsCapability(capability)
}

func init() {
	flavorFuncs[replication.FilePosFlavorID] = newFilePosFlavor
}

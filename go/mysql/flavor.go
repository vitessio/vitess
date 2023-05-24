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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	// ErrNotReplica means there is no replication status.
	// Returned by ShowReplicationStatus().
	ErrNotReplica = NewSQLError(ERNotReplica, SSUnknownSQLState, "no replication status")

	// ErrNoPrimaryStatus means no status was returned by ShowPrimaryStatus().
	ErrNoPrimaryStatus = errors.New("no master status")
)

type FlavorCapability int

const (
	NoneFlavorCapability          FlavorCapability = iota // default placeholder
	FastDropTableFlavorCapability                         // supported in MySQL 8.0.23 and above: https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-23.html
	TransactionalGtidExecutedFlavorCapability
	InstantDDLFlavorCapability
	InstantAddLastColumnFlavorCapability
	InstantAddDropVirtualColumnFlavorCapability
	InstantAddDropColumnFlavorCapability
	InstantChangeColumnDefaultFlavorCapability
	InstantExpandEnumCapability
	MySQLJSONFlavorCapability
	MySQLUpgradeInServerFlavorCapability
	DynamicRedoLogCapacityFlavorCapability // supported in MySQL 8.0.30 and above: https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-30.html
	DisableRedoLogFlavorCapability         // supported in MySQL 8.0.21 and above: https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-21.html
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
	primaryGTIDSet(c *Conn) (GTIDSet, error)

	// purgedGTIDSet returns the purged GTIDSet of a server.
	purgedGTIDSet(c *Conn) (GTIDSet, error)

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
	startReplicationUntilAfter(pos Position) string

	// startSQLThreadUntilAfter will start replication's sql thread(s), but only allow it
	// to run until `pos` is reached. After reaching pos, it will be stopped again
	startSQLThreadUntilAfter(pos Position) string

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
	sendBinlogDumpCommand(c *Conn, serverID uint32, binlogFilename string, startPos Position) error

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
	setReplicationPositionCommands(pos Position) []string

	// changeReplicationSourceArg returns the specific parameter to add to
	// a "change primary" command.
	changeReplicationSourceArg() string

	// status returns the result of the appropriate status command,
	// with parsed replication position.
	status(c *Conn) (ReplicationStatus, error)

	// primaryStatus returns the result of 'SHOW MASTER STATUS',
	// with parsed executed position.
	primaryStatus(c *Conn) (PrimaryStatus, error)

	// waitUntilPositionCommand returns the SQL command to issue
	// to wait until the given position, until the context
	// expires.  The command returns -1 if it times out. It
	// returns NULL if GTIDs are not enabled.
	waitUntilPositionCommand(ctx context.Context, pos Position) (string, error)

	baseShowTables() string
	baseShowTablesWithSizes() string

	supportsCapability(serverVersion string, capability FlavorCapability) (bool, error)
}

type CapableOf func(capability FlavorCapability) (bool, error)

// flavors maps flavor names to their implementation.
// Flavors need to register only if they support being specified in the
// connection parameters.
var flavors = make(map[string]func() flavor)

// ServerVersionAtLeast returns true if current server is at least given value.
// Example: if input is []int{8, 0, 23}... the function returns 'true' if we're on MySQL 8.0.23, 8.0.24, ...
func ServerVersionAtLeast(serverVersion string, parts ...int) (bool, error) {
	versionPrefix := strings.Split(serverVersion, "-")[0]
	versionTokens := strings.Split(versionPrefix, ".")
	for i, part := range parts {
		if len(versionTokens) <= i {
			return false, nil
		}
		tokenValue, err := strconv.Atoi(versionTokens[i])
		if err != nil {
			return false, err
		}
		if tokenValue > part {
			return true, nil
		}
		if tokenValue < part {
			return false, nil
		}
	}
	return true, nil
}

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
func GetFlavor(serverVersion string, flavorFunc func() flavor) (f flavor, capableOf CapableOf, canonicalVersion string) {
	canonicalVersion = serverVersion
	switch {
	case flavorFunc != nil:
		f = flavorFunc()
	case strings.HasPrefix(serverVersion, mariaDBReplicationHackPrefix):
		canonicalVersion = serverVersion[len(mariaDBReplicationHackPrefix):]
		f = mariadbFlavor101{}
	case strings.Contains(serverVersion, mariaDBVersionString):
		mariadbVersion, err := strconv.ParseFloat(serverVersion[:4], 64)
		if err != nil || mariadbVersion < 10.2 {
			f = mariadbFlavor101{}
		} else {
			f = mariadbFlavor102{}
		}
	case strings.HasPrefix(serverVersion, mysql57VersionPrefix):
		f = mysqlFlavor57{}
	case strings.HasPrefix(serverVersion, mysql80VersionPrefix):
		f = mysqlFlavor80{}
	default:
		f = mysqlFlavor56{}
	}
	return f,
		func(capability FlavorCapability) (bool, error) {
			return f.supportsCapability(serverVersion, capability)
		}, canonicalVersion
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
	flavorFunc := flavors[params.Flavor]
	c.flavor, _, c.ServerVersion = GetFlavor(c.ServerVersion, flavorFunc)
}

// ServerVersionAtLeast returns 'true' if server version is equal or greater than given parts. e.g.
// "8.0.14-log" is at least [8, 0, 13] and [8, 0, 14], but not [8, 0, 15]
func (c *Conn) ServerVersionAtLeast(parts ...int) (bool, error) {
	return ServerVersionAtLeast(c.ServerVersion, parts...)
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
func (c *Conn) PrimaryPosition() (Position, error) {
	gtidSet, err := c.flavor.primaryGTIDSet(c)
	if err != nil {
		return Position{}, err
	}
	return Position{
		GTIDSet: gtidSet,
	}, nil
}

// GetGTIDPurged returns the tablet's GTIDs which are purged.
func (c *Conn) GetGTIDPurged() (Position, error) {
	gtidSet, err := c.flavor.purgedGTIDSet(c)
	if err != nil {
		return Position{}, err
	}
	return Position{
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
func (c *Conn) PrimaryFilePosition() (Position, error) {
	filePosFlavor := filePosFlavor{}
	gtidSet, err := filePosFlavor.primaryGTIDSet(c)
	if err != nil {
		return Position{}, err
	}
	return Position{
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
func (c *Conn) StartReplicationUntilAfterCommand(pos Position) string {
	return c.flavor.startReplicationUntilAfter(pos)
}

// StartSQLThreadUntilAfterCommand returns the command to start the replica's SQL
// thread(s) and have it run until it has reached the given position, at which point
// it will stop.
func (c *Conn) StartSQLThreadUntilAfterCommand(pos Position) string {
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
func (c *Conn) SendBinlogDumpCommand(serverID uint32, binlogFilename string, startPos Position) error {
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
func (c *Conn) SetReplicationPositionCommands(pos Position) []string {
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

// parseReplicationStatus parses the common (non-flavor-specific) fields of ReplicationStatus
func parseReplicationStatus(fields map[string]string) ReplicationStatus {
	// The field names in the map are identical to what we receive from the database
	// Hence the names still contain Master
	status := ReplicationStatus{
		SourceHost:            fields["Master_Host"],
		SourceUser:            fields["Master_User"],
		SSLAllowed:            fields["Master_SSL_Allowed"] == "Yes",
		AutoPosition:          fields["Auto_Position"] == "1",
		UsingGTID:             fields["Using_Gtid"] != "No" && fields["Using_Gtid"] != "",
		HasReplicationFilters: (fields["Replicate_Do_DB"] != "") || (fields["Replicate_Ignore_DB"] != "") || (fields["Replicate_Do_Table"] != "") || (fields["Replicate_Ignore_Table"] != "") || (fields["Replicate_Wild_Do_Table"] != "") || (fields["Replicate_Wild_Ignore_Table"] != ""),
		// These fields are returned from the underlying DB and cannot be renamed
		IOState:      ReplicationStatusToState(fields["Slave_IO_Running"]),
		LastIOError:  fields["Last_IO_Error"],
		SQLState:     ReplicationStatusToState(fields["Slave_SQL_Running"]),
		LastSQLError: fields["Last_SQL_Error"],
	}
	parseInt, _ := strconv.ParseInt(fields["Master_Port"], 10, 32)
	status.SourcePort = int32(parseInt)
	parseInt, _ = strconv.ParseInt(fields["Connect_Retry"], 10, 32)
	status.ConnectRetry = int32(parseInt)
	parseUint, err := strconv.ParseUint(fields["Seconds_Behind_Master"], 10, 32)
	if err != nil {
		// we could not parse the value into a valid uint32 -- most commonly because the value is NULL from the
		// database -- so let's reflect that the underlying value was unknown on our last check
		status.ReplicationLagUnknown = true
	} else {
		status.ReplicationLagUnknown = false
		status.ReplicationLagSeconds = uint32(parseUint)
	}
	parseUint, _ = strconv.ParseUint(fields["Master_Server_Id"], 10, 32)
	status.SourceServerID = uint32(parseUint)
	parseUint, _ = strconv.ParseUint(fields["SQL_Delay"], 10, 32)
	status.SQLDelay = uint32(parseUint)

	executedPosStr := fields["Exec_Master_Log_Pos"]
	file := fields["Relay_Master_Log_File"]
	if file != "" && executedPosStr != "" {
		filePos, err := strconv.ParseUint(executedPosStr, 10, 32)
		if err == nil {
			status.FilePosition.GTIDSet = filePosGTID{
				file: file,
				pos:  uint32(filePos),
			}
		}
	}

	readPosStr := fields["Read_Master_Log_Pos"]
	file = fields["Master_Log_File"]
	if file != "" && readPosStr != "" {
		fileRelayPos, err := strconv.ParseUint(readPosStr, 10, 32)
		if err == nil {
			status.RelayLogSourceBinlogEquivalentPosition.GTIDSet = filePosGTID{
				file: file,
				pos:  uint32(fileRelayPos),
			}
		}
	}

	relayPosStr := fields["Relay_Log_Pos"]
	file = fields["Relay_Log_File"]
	if file != "" && relayPosStr != "" {
		relayFilePos, err := strconv.ParseUint(relayPosStr, 10, 32)
		if err == nil {
			status.RelayLogFilePosition.GTIDSet = filePosGTID{
				file: file,
				pos:  uint32(relayFilePos),
			}
		}
	}
	return status
}

// ShowReplicationStatus executes the right command to fetch replication status,
// and returns a parsed Position with other fields.
func (c *Conn) ShowReplicationStatus() (ReplicationStatus, error) {
	return c.flavor.status(c)
}

// parsePrimaryStatus parses the common fields of SHOW MASTER STATUS.
func parsePrimaryStatus(fields map[string]string) PrimaryStatus {
	status := PrimaryStatus{}

	fileExecPosStr := fields["Position"]
	file := fields["File"]
	if file != "" && fileExecPosStr != "" {
		filePos, err := strconv.ParseUint(fileExecPosStr, 10, 32)
		if err == nil {
			status.FilePosition.GTIDSet = filePosGTID{
				file: file,
				pos:  uint32(filePos),
			}
		}
	}

	return status
}

// ShowPrimaryStatus executes the right SHOW MASTER STATUS command,
// and returns a parsed executed Position, as well as file based Position.
func (c *Conn) ShowPrimaryStatus() (PrimaryStatus, error) {
	return c.flavor.primaryStatus(c)
}

// WaitUntilPositionCommand returns the SQL command to issue
// to wait until the given position, until the context
// expires.  The command returns -1 if it times out. It
// returns NULL if GTIDs are not enabled.
func (c *Conn) WaitUntilPositionCommand(ctx context.Context, pos Position) (string, error) {
	return c.flavor.waitUntilPositionCommand(ctx, pos)
}

// WaitUntilFilePositionCommand returns the SQL command to issue
// to wait until the given position, until the context
// expires for the file position flavor.  The command returns -1 if it times out. It
// returns NULL if GTIDs are not enabled.
func (c *Conn) WaitUntilFilePositionCommand(ctx context.Context, pos Position) (string, error) {
	filePosFlavor := filePosFlavor{}
	return filePosFlavor.waitUntilPositionCommand(ctx, pos)
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
func (c *Conn) SupportsCapability(capability FlavorCapability) (bool, error) {
	return c.flavor.supportsCapability(c.ServerVersion, capability)
}

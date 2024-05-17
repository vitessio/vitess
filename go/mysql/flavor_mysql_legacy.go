/*
Copyright 2024 The Vitess Authors.

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

	"vitess.io/vitess/go/mysql/replication"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// mysqlFlavorLegacy implements the Flavor interface for Mysql for
// older versions. This applies to MySQL 5.7 and early 8.0 versions from
// before the replication terminology deprecation.
type mysqlFlavorLegacy struct {
	mysqlFlavor
}

// mysqlFlavor57 is the explicit flavor for MySQL 5.7. It's basically
// the same as the legacy flavor, but it has a separate name here to
// be explicit about the version.
type mysqlFlavor57 struct {
	mysqlFlavorLegacy
}

// mysqlFlavor8 is the explicit flavor for MySQL 8.0. It's similarly to
// 5.7 the same as the legacy flavor, but has an explicit name to be
// clear it's used for early MySQL 8.0 versions.
type mysqlFlavor8Legacy struct {
	mysqlFlavorLegacy
}

var _ flavor = (*mysqlFlavor57)(nil)
var _ flavor = (*mysqlFlavor8Legacy)(nil)
var _ flavor = (*mysqlFlavor8)(nil)

// TablesWithSize56 is a query to select table along with size for mysql 5.6
const TablesWithSize56 = `SELECT table_name,
	table_type,
	UNIX_TIMESTAMP(create_time) AS uts_create_time,
	table_comment,
	SUM(data_length + index_length),
	SUM(data_length + index_length)
FROM information_schema.tables
WHERE table_schema = database()
GROUP BY table_name,
	table_type,
	uts_create_time,
	table_comment`

// TablesWithSize57 is a query to select table along with size for mysql 5.7.
//
// It's a little weird, because the JOIN predicate only works if the table and databases do not contain weird characters.
// If the join does not return any data, we fall back to the same fields as used in the mysql 5.6 query.
//
// We join with a subquery that materializes the data from `information_schema.innodb_sys_tablespaces`
// early for performance reasons. This effectively causes only a single read of `information_schema.innodb_sys_tablespaces`
// per query.
const TablesWithSize57 = `SELECT t.table_name,
	t.table_type,
	UNIX_TIMESTAMP(t.create_time),
	t.table_comment,
	IFNULL(SUM(i.file_size), SUM(t.data_length + t.index_length)),
	IFNULL(SUM(i.allocated_size), SUM(t.data_length + t.index_length))
FROM information_schema.tables t
LEFT OUTER JOIN (
	SELECT space, file_size, allocated_size, name
	FROM information_schema.innodb_sys_tablespaces
	WHERE name LIKE CONCAT(database(), '/%')
	GROUP BY space, file_size, allocated_size, name
) i ON i.name = CONCAT(t.table_schema, '/', t.table_name) or i.name LIKE CONCAT(t.table_schema, '/', t.table_name, '#p#%')
WHERE t.table_schema = database()
GROUP BY t.table_name, t.table_type, t.create_time, t.table_comment`

func (mysqlFlavorLegacy) startReplicationCommand() string {
	return "START SLAVE"
}

func (mysqlFlavorLegacy) restartReplicationCommands() []string {
	return []string{
		"STOP SLAVE",
		"RESET SLAVE",
		"START SLAVE",
	}
}

func (mysqlFlavorLegacy) startReplicationUntilAfter(pos replication.Position) string {
	return fmt.Sprintf("START SLAVE UNTIL SQL_AFTER_GTIDS = '%s'", pos)
}

func (mysqlFlavorLegacy) startSQLThreadUntilAfter(pos replication.Position) string {
	return fmt.Sprintf("START SLAVE SQL_THREAD UNTIL SQL_AFTER_GTIDS = '%s'", pos)
}

func (mysqlFlavorLegacy) stopReplicationCommand() string {
	return "STOP SLAVE"
}

func (mysqlFlavorLegacy) resetReplicationCommand() string {
	return "RESET SLAVE ALL"
}

func (mysqlFlavorLegacy) stopIOThreadCommand() string {
	return "STOP SLAVE IO_THREAD"
}

func (mysqlFlavorLegacy) stopSQLThreadCommand() string {
	return "STOP SLAVE SQL_THREAD"
}

func (mysqlFlavorLegacy) startSQLThreadCommand() string {
	return "START SLAVE SQL_THREAD"
}

// resetReplicationCommands is part of the Flavor interface.
func (mysqlFlavorLegacy) resetReplicationCommands(c *Conn) []string {
	resetCommands := []string{
		"STOP SLAVE",
		"RESET SLAVE ALL", // "ALL" makes it forget source host:port.
		"RESET MASTER",    // This will also clear gtid_executed and gtid_purged.
	}
	status, err := c.SemiSyncExtensionLoaded()
	if err != nil {
		return resetCommands
	}
	switch status {
	case SemiSyncTypeSource:
		resetCommands = append(resetCommands, "SET GLOBAL rpl_semi_sync_source_enabled = false, GLOBAL rpl_semi_sync_replica_enabled = false") // semi-sync will be enabled if needed when replica is started.
	case SemiSyncTypeMaster:
		resetCommands = append(resetCommands, "SET GLOBAL rpl_semi_sync_master_enabled = false, GLOBAL rpl_semi_sync_slave_enabled = false") // semi-sync will be enabled if needed when replica is started.
	default:
		// Nothing to do.
	}
	return resetCommands
}

// resetReplicationParametersCommands is part of the Flavor interface.
func (mysqlFlavorLegacy) resetReplicationParametersCommands(c *Conn) []string {
	resetCommands := []string{
		"RESET SLAVE ALL", // "ALL" makes it forget source host:port.
	}
	return resetCommands
}

// status is part of the Flavor interface.
func (mysqlFlavorLegacy) status(c *Conn) (replication.ReplicationStatus, error) {
	qr, err := c.ExecuteFetch("SHOW SLAVE STATUS", 100, true /* wantfields */)
	if err != nil {
		return replication.ReplicationStatus{}, err
	}
	if len(qr.Rows) == 0 {
		// The query returned no data, meaning the server
		// is not configured as a replica.
		return replication.ReplicationStatus{}, ErrNotReplica
	}

	resultMap, err := resultToMap(qr)
	if err != nil {
		return replication.ReplicationStatus{}, err
	}

	return replication.ParseMysqlReplicationStatus(resultMap, false)
}

// replicationNetTimeout is part of the Flavor interface.
func (mysqlFlavorLegacy) replicationNetTimeout(c *Conn) (int32, error) {
	qr, err := c.ExecuteFetch("select @@global.slave_net_timeout", 1, false)
	if err != nil {
		return 0, err
	}
	if len(qr.Rows) != 1 || len(qr.Rows[0]) != 1 {
		return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected result format for slave_net_timeout: %#v", qr)
	}
	return qr.Rows[0][0].ToInt32()
}

func (mysqlFlavorLegacy) catchupToGTIDCommands(params *ConnParams, replPos replication.Position) []string {
	cmds := []string{
		"STOP SLAVE FOR CHANNEL '' ",
		"STOP SLAVE IO_THREAD FOR CHANNEL ''",
	}

	if params.SslCa != "" || params.SslCert != "" {
		// We need to use TLS
		cmd := fmt.Sprintf("CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d, MASTER_USER='%s', MASTER_PASSWORD='%s', MASTER_AUTO_POSITION=1, MASTER_SSL=1", params.Host, params.Port, params.Uname, params.Pass)
		if params.SslCa != "" {
			cmd += fmt.Sprintf(", MASTER_SSL_CA='%s'", params.SslCa)
		}
		if params.SslCert != "" {
			cmd += fmt.Sprintf(", MASTER_SSL_CERT='%s'", params.SslCert)
		}
		if params.SslKey != "" {
			cmd += fmt.Sprintf(", MASTER_SSL_KEY='%s'", params.SslKey)
		}
		cmds = append(cmds, cmd+";")
	} else {
		// No TLS
		cmds = append(cmds, fmt.Sprintf("CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d, MASTER_USER='%s', MASTER_PASSWORD='%s', MASTER_AUTO_POSITION=1;", params.Host, params.Port, params.Uname, params.Pass))
	}

	if replPos.IsZero() { // when the there is no afterPos, that means need to replicate completely
		cmds = append(cmds, "START SLAVE")
	} else {
		cmds = append(cmds, fmt.Sprintf("START SLAVE UNTIL SQL_BEFORE_GTIDS = '%s'", replPos.GTIDSet.Last()))
	}
	return cmds
}

func (mysqlFlavorLegacy) setReplicationSourceCommand(params *ConnParams, host string, port int32, heartbeatInterval float64, connectRetry int) string {
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
	if heartbeatInterval != 0 {
		args = append(args, fmt.Sprintf("MASTER_HEARTBEAT_PERIOD = %v", heartbeatInterval))
	}
	args = append(args, "MASTER_AUTO_POSITION = 1")
	return "CHANGE MASTER TO\n  " + strings.Join(args, ",\n  ")
}

func (mysqlFlavorLegacy) resetBinaryLogsCommand() string {
	return "RESET MASTER"
}

func (mysqlFlavorLegacy) binlogReplicatedUpdates() string {
	return "@@global.log_slave_updates"
}

// setReplicationPositionCommands is part of the Flavor interface.
func (mysqlFlavorLegacy) setReplicationPositionCommands(pos replication.Position) []string {
	return []string{
		"RESET MASTER", // We must clear gtid_executed before setting gtid_purged.
		fmt.Sprintf("SET GLOBAL gtid_purged = '%s'", pos),
	}
}

// primaryStatus is part of the Flavor interface.
func (mysqlFlavorLegacy) primaryStatus(c *Conn) (replication.PrimaryStatus, error) {
	qr, err := c.ExecuteFetch("SHOW MASTER STATUS", 100, true /* wantfields */)
	if err != nil {
		return replication.PrimaryStatus{}, err
	}
	if len(qr.Rows) == 0 {
		// The query returned no data. We don't know how this could happen.
		return replication.PrimaryStatus{}, ErrNoPrimaryStatus
	}

	resultMap, err := resultToMap(qr)
	if err != nil {
		return replication.PrimaryStatus{}, err
	}

	return replication.ParseMysqlPrimaryStatus(resultMap)
}

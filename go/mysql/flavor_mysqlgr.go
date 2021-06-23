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
	"fmt"
	"math"

	"vitess.io/vitess/go/sqltypes"
)

// GRFlavorID is the string identifier for the MysqlGR flavor.
const GRFlavorID = "MysqlGR"

// mysqlGRFlavor implements the Flavor interface for Mysql.
type mysqlGRFlavor struct {
	mysqlFlavor
}

// newMysqlGRFlavor creates a new mysqlGR flavor.
func newMysqlGRFlavor() flavor {
	return &mysqlGRFlavor{}
}

// startReplicationCommand returns the command to start the replication.
// we return empty here since `START GROUP_REPLICATION` should be called by
// the external orchestrator
func (mysqlGRFlavor) startReplicationCommand() string {
	return ""
}

// restartReplicationCommands returns the commands to stop, reset and start the replication.
// for mysqlGRFlavor we don't need this functionality
func (mysqlGRFlavor) restartReplicationCommands() []string {
	return []string{}
}

// startReplicationUntilAfter will restart replication, but only allow it
// to run until `pos` is reached. After reaching pos, replication will be stopped again
func (mysqlGRFlavor) startReplicationUntilAfter(pos Position) string {
	return ""
}

// stopReplicationCommand returns the command to stop the replication.
func (mysqlGRFlavor) stopReplicationCommand() string {
	return "STOP GROUP_REPLICATION"
}

// stopIOThreadCommand returns the command to stop the replica's io thread only.
func (mysqlGRFlavor) stopIOThreadCommand() string {
	return ""
}

// resetReplicationCommands returns the commands to completely reset
// replication on the host.
func (mysqlGRFlavor) resetReplicationCommands(c *Conn) []string {
	return []string{}
}

// setReplicationPositionCommands returns the commands to set the
// replication position at which the replica will resume.
func (mysqlGRFlavor) setReplicationPositionCommands(pos Position) []string {
	return []string{}
}

// status returns the result of the appropriate status command,
// with parsed replication position.
//
// Note: primary will skip this function, only replica will call it.
// TODO: Right now the GR's lag is defined as the lag between a node processing a txn
// and the time the txn was committed. We should consider reporting lag between current queueing txn timestamp
// from replication_connection_status and the current processing txn's commit timestamp
func (mysqlGRFlavor) status(c *Conn) (ReplicationStatus, error) {
	res := ReplicationStatus{}
	// Get master node information
	query := `SELECT
		MEMBER_HOST,
		MEMBER_PORT
	FROM
		performance_schema.replication_group_members
	WHERE
		MEMBER_ROLE='PRIMARY' AND MEMBER_STATE='ONLINE'`
	err := fetchStatusForGroupReplication(c, query, func(values []sqltypes.Value) error {
		parsePrimaryGroupMember(&res, values)
		return nil
	})
	if err != nil {
		return ReplicationStatus{}, err
	}

	query = `SELECT
		MEMBER_STATE
	FROM
		performance_schema.replication_group_members
	WHERE
		MEMBER_HOST=convert(@@hostname using ascii) AND MEMBER_PORT=@@port`
	var chanel string
	err = fetchStatusForGroupReplication(c, query, func(values []sqltypes.Value) error {
		state := values[0].ToString()
		if state == "ONLINE" {
			chanel = "group_replication_applier"
		} else if state == "RECOVERING" {
			chanel = "group_replication_recovery"
		} else { // OFFLINE, ERROR, UNREACHABLE
			// If the member is not in healthy state, use max int as lag
			res.SecondsBehindMaster = math.MaxUint32
		}
		return nil
	})
	if err != nil {
		return ReplicationStatus{}, err
	}
	// if chanel is not set, it means the state is not ONLINE or RECOVERING
	// return partial result early
	if chanel == "" {
		return res, nil
	}

	// Populate IOThreadRunning from replication_connection_status
	query = fmt.Sprintf(`SELECT SERVICE_STATE
		FROM performance_schema.replication_connection_status
		WHERE CHANNEL_NAME='%s'`, chanel)
	var ioThreadRunning bool
	err = fetchStatusForGroupReplication(c, query, func(values []sqltypes.Value) error {
		ioThreadRunning = values[0].ToString() == "ON"
		return nil
	})
	if err != nil {
		return ReplicationStatus{}, err
	}
	res.IOThreadRunning = ioThreadRunning
	// Populate SQLThreadRunning from replication_connection_status
	var sqlThreadRunning bool
	query = fmt.Sprintf(`SELECT SERVICE_STATE
		FROM performance_schema.replication_applier_status_by_coordinator
		WHERE CHANNEL_NAME='%s'`, chanel)
	err = fetchStatusForGroupReplication(c, query, func(values []sqltypes.Value) error {
		sqlThreadRunning = values[0].ToString() == "ON"
		return nil
	})
	if err != nil {
		return ReplicationStatus{}, err
	}
	res.SQLThreadRunning = sqlThreadRunning

	// Collect lag information
	// we use the difference between the last processed transaction's commit time
	// and the end buffer time as the proxy to the lag
	query = fmt.Sprintf(`SELECT
		TIMESTAMPDIFF(SECOND, LAST_PROCESSED_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP, LAST_PROCESSED_TRANSACTION_END_BUFFER_TIMESTAMP)
	FROM
		performance_schema.replication_applier_status_by_coordinator
	WHERE
		CHANNEL_NAME='%s'`, chanel)
	err = fetchStatusForGroupReplication(c, query, func(values []sqltypes.Value) error {
		parseReplicationApplierLag(&res, values)
		return nil
	})
	if err != nil {
		return ReplicationStatus{}, err
	}
	return res, nil
}

func parsePrimaryGroupMember(res *ReplicationStatus, row []sqltypes.Value) {
	res.MasterHost = row[0].ToString() /* MEMBER_HOST */
	memberPort, _ := row[1].ToInt64()  /* MEMBER_PORT */
	res.MasterPort = int(memberPort)
}

func parseReplicationApplierLag(res *ReplicationStatus, row []sqltypes.Value) {
	lagSec, err := row[0].ToInt64()
	// if the error is not nil, SecondsBehindMaster will remain to be MaxUint32
	if err == nil {
		// Only set where there is no error
		// The value can be NULL when there is no replication applied yet
		res.SecondsBehindMaster = uint(lagSec)
	}
}

func fetchStatusForGroupReplication(c *Conn, query string, onResult func([]sqltypes.Value) error) error {
	qr, err := c.ExecuteFetch(query, 100, true /* wantfields */)
	if err != nil {
		return err
	}
	// if group replication related query returns 0 rows, it means the group replication is not set up
	if len(qr.Rows) == 0 {
		return ErrNoGroupStatus
	}
	if len(qr.Rows) > 1 {
		return fmt.Errorf("unexpected results for %v: %v", query, qr.Rows)
	}
	return onResult(qr.Rows[0])
}

// masterStatus returns the result of 'SHOW MASTER STATUS',
// with parsed executed position.
func (mysqlGRFlavor) masterStatus(c *Conn) (PrimaryStatus, error) {
	return mysqlFlavor{}.primaryStatus(c)
}

func (mysqlGRFlavor) baseShowTablesWithSizes() string {
	return TablesWithSize80
}

func init() {
	flavors[GRFlavorID] = newMysqlGRFlavor
}

/*
Copyright 2026 The Vitess Authors.

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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/log"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	fullStatusGlobalVariablesQuery = "SELECT variable_name, variable_value FROM performance_schema.global_variables WHERE variable_name IN %a"
	fullStatusGlobalStatusQuery    = "SELECT variable_name, variable_value FROM performance_schema.global_status WHERE variable_name IN %a"
)

type (
	// FullStatusResult contains the FullStatus proto assembled by
	// CollectFullStatusData along with any non-fatal errors collected while
	// reading semi-sync data.
	FullStatusResult struct {
		Status     *replicationdatapb.FullStatus
		SoftErrors []error
	}
)

var (
	fullStatusSemiSyncVariables = []string{
		"rpl_semi_sync_source_enabled",
		"rpl_semi_sync_replica_enabled",
		"rpl_semi_sync_source_timeout",
		"rpl_semi_sync_source_wait_for_replica_count",
		"rpl_semi_sync_master_enabled",
		"rpl_semi_sync_slave_enabled",
		"rpl_semi_sync_master_timeout",
		"rpl_semi_sync_master_wait_for_slave_count",
	}
	fullStatusSemiSyncStatuses = []string{
		"Rpl_semi_sync_source_status",
		"Rpl_semi_sync_replica_status",
		"Rpl_semi_sync_source_clients",
		"Rpl_semi_sync_master_status",
		"Rpl_semi_sync_slave_status",
		"Rpl_semi_sync_master_clients",
	}
)

// CollectFullStatusData collects FullStatus data on one connection for
// supported MySQL flavors. A nil result (with a nil error) means the caller
// should use the existing collection path.
func (mysqld *Mysqld) CollectFullStatusData(ctx context.Context) (*FullStatusResult, error) {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()

	if conn.Conn.IsMariaDB() {
		return nil, nil
	}

	variables, err := mysqld.fetchFullStatusVariables(ctx, conn)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		log.Warn("FullStatus optimized collection failed; falling back to legacy collection",
			slog.String("recovery", "legacy_full_status"),
			slog.Any("error", err))
		return nil, nil
	}

	_, parsedVersion, err := ParseVersionString(versionStringPrefix + variables.Version)
	if err != nil {
		return nil, err
	}

	result := &FullStatusResult{
		Status: &replicationdatapb.FullStatus{
			ServerId:          variables.ServerID,
			ServerUuid:        variables.ServerUUID,
			Version:           fmt.Sprintf("%d.%d.%d", parsedVersion.Major, parsedVersion.Minor, parsedVersion.Patch),
			VersionComment:    variables.VersionComment,
			ReadOnly:          variables.ReadOnly,
			SuperReadOnly:     variables.SuperReadOnly,
			GtidMode:          variables.GTIDMode,
			BinlogFormat:      variables.BinlogFormat,
			BinlogRowImage:    variables.BinlogRowImage,
			LogBinEnabled:     variables.LogBin,
			LogReplicaUpdates: variables.LogReplicaUpdates,
		},
	}
	status := result.Status

	var replicationStatus replication.ReplicationStatus
	err = runFullStatusQuery(ctx, conn, "replication status", func() error {
		var queryErr error
		replicationStatus, queryErr = conn.Conn.ShowReplicationStatus()
		return queryErr
	})
	if err != nil && err != mysql.ErrNotReplica {
		return nil, err
	}
	if err == nil {
		status.ReplicationStatus = replication.ReplicationStatusToProto(replicationStatus)
	}

	var primaryStatus replication.PrimaryStatus
	err = runFullStatusQuery(ctx, conn, "primary status", func() error {
		var queryErr error
		primaryStatus, queryErr = conn.Conn.ShowPrimaryStatus()
		return queryErr
	})
	if err != nil && err != mysql.ErrNoPrimaryStatus {
		return nil, err
	}
	if err == nil {
		primaryStatus.ServerUUID = variables.ServerUUID
		status.PrimaryStatus = replication.PrimaryStatusToProto(primaryStatus)
	}

	var gtidPurged replication.Position
	err = runFullStatusQuery(ctx, conn, "GTID purged", func() error {
		var queryErr error
		gtidPurged, queryErr = conn.Conn.GetGTIDPurged()
		return queryErr
	})
	if err != nil {
		return nil, err
	}
	status.GtidPurged = replication.EncodePosition(gtidPurged)

	if err := mysqld.collectFullStatusSemiSync(ctx, conn, result); err != nil {
		return nil, err
	}

	err = runFullStatusQuery(ctx, conn, "replication configuration", func() error {
		var queryErr error
		status.ReplicationConfiguration, queryErr = conn.Conn.ReplicationConfiguration()
		return queryErr
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

// runFullStatusQuery runs a FullStatus query and retries once after reconnecting
// when MySQL drops the connection.
func runFullStatusQuery(ctx context.Context, conn *dbconnpool.PooledDBConnection, queryName string, query func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	err := query()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if err == nil {
		return nil
	}

	sqlErr, ok := errors.AsType[*sqlerror.SQLError](err)
	if !ok || (sqlErr.Number() != sqlerror.CRServerGone && sqlErr.Number() != sqlerror.CRServerLost) {
		return err
	}

	log.Warn("FullStatus query lost its MySQL connection; reconnecting before retry",
		slog.String("workflow", "full_status"),
		slog.String("query", queryName),
		slog.String("recovery", "reconnect"),
		slog.Any("error", err))
	if err := conn.Conn.Reconnect(ctx); err != nil {
		return vterrors.Wrapf(err, "failed to reconnect while collecting FullStatus %s", queryName)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	err = query()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return err
}

// fetchFullStatusVariables reads and parses the mandatory FullStatus variables.
func (mysqld *Mysqld) fetchFullStatusVariables(ctx context.Context, conn *dbconnpool.PooledDBConnection) (*mysql.FullStatusVariables, error) {
	qr, err := mysqld.executeFetchContext(ctx, conn, conn.Conn.FullStatusVariablesQuery(), 1, true)
	if err != nil {
		return nil, err
	}
	return mysql.ParseFullStatusVariables(qr)
}

// collectFullStatusSemiSync fills the optional semi-sync fields. Query and
// parsing errors are retained as soft errors, while context cancellation is fatal.
// Both reads retry after a reconnect like the mandatory queries do, so that a
// dropped connection does not report semi-sync as disabled and off.
func (mysqld *Mysqld) collectFullStatusSemiSync(ctx context.Context, conn *dbconnpool.PooledDBConnection, result *FullStatusResult) error {
	var variables map[string]string
	err := runFullStatusQuery(ctx, conn, "semi-sync variables", func() error {
		var queryErr error
		variables, queryErr = mysqld.fetchFullStatusValues(ctx, conn, fullStatusGlobalVariablesQuery, fullStatusSemiSyncVariables)
		return queryErr
	})
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		result.SoftErrors = append(result.SoftErrors, vterrors.Wrap(err, "failed to read semi-sync variables"))
	} else {
		result.parseSemiSyncVariables(variables)
	}

	var statuses map[string]string
	err = runFullStatusQuery(ctx, conn, "semi-sync status", func() error {
		var queryErr error
		statuses, queryErr = mysqld.fetchFullStatusValues(ctx, conn, fullStatusGlobalStatusQuery, fullStatusSemiSyncStatuses)
		return queryErr
	})
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		result.SoftErrors = append(result.SoftErrors, vterrors.Wrap(err, "failed to read semi-sync status"))
	} else {
		result.parseSemiSyncStatuses(statuses)
	}
	return nil
}

// fetchFullStatusValues reads the requested variables or status values from
// performance_schema.
func (mysqld *Mysqld) fetchFullStatusValues(ctx context.Context, conn *dbconnpool.PooledDBConnection, queryTemplate string, names []string) (map[string]string, error) {
	bv, err := sqltypes.BuildBindVariable(names)
	if err != nil {
		return nil, err
	}
	query, err := sqlparser.ParseAndBind(queryTemplate, bv)
	if err != nil {
		return nil, err
	}
	qr, err := mysqld.executeFetchContext(ctx, conn, query, len(names), false)
	if err != nil {
		return nil, err
	}
	values := make(map[string]string, len(qr.Rows))
	for _, row := range qr.Rows {
		if len(row) != 2 {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "semi-sync query returned %d fields, expected 2", len(row))
		}
		values[row[0].ToString()] = row[1].ToString()
	}
	return values, nil
}

// parseSemiSyncVariables fills the semi-sync settings using either source/replica
// or the legacy master/slave names.
func (result *FullStatusResult) parseSemiSyncVariables(values map[string]string) {
	status := result.Status
	if _, ok := values["rpl_semi_sync_source_enabled"]; ok {
		status.SemiSyncPrimaryEnabled = values["rpl_semi_sync_source_enabled"] == "ON"
		status.SemiSyncReplicaEnabled = values["rpl_semi_sync_replica_enabled"] == "ON"
		status.SemiSyncPrimaryTimeout = result.parseOptionalUint(values, "rpl_semi_sync_source_timeout", 64)
		status.SemiSyncWaitForReplicaCount = uint32(result.parseOptionalUint(values, "rpl_semi_sync_source_wait_for_replica_count", 32))
		return
	}
	if _, ok := values["rpl_semi_sync_master_enabled"]; ok {
		status.SemiSyncPrimaryEnabled = values["rpl_semi_sync_master_enabled"] == "ON"
		status.SemiSyncReplicaEnabled = values["rpl_semi_sync_slave_enabled"] == "ON"
		status.SemiSyncPrimaryTimeout = result.parseOptionalUint(values, "rpl_semi_sync_master_timeout", 64)
		status.SemiSyncWaitForReplicaCount = uint32(result.parseOptionalUint(values, "rpl_semi_sync_master_wait_for_slave_count", 32))
	}
}

// parseSemiSyncStatuses fills the semi-sync status using either source/replica
// or the legacy master/slave names.
func (result *FullStatusResult) parseSemiSyncStatuses(values map[string]string) {
	status := result.Status
	if _, ok := values["Rpl_semi_sync_source_status"]; ok {
		status.SemiSyncPrimaryStatus = values["Rpl_semi_sync_source_status"] == "ON"
		status.SemiSyncReplicaStatus = values["Rpl_semi_sync_replica_status"] == "ON"
		status.SemiSyncPrimaryClients = uint32(result.parseOptionalUint(values, "Rpl_semi_sync_source_clients", 32))
		return
	}
	if _, ok := values["Rpl_semi_sync_master_status"]; ok {
		status.SemiSyncPrimaryStatus = values["Rpl_semi_sync_master_status"] == "ON"
		status.SemiSyncReplicaStatus = values["Rpl_semi_sync_slave_status"] == "ON"
		status.SemiSyncPrimaryClients = uint32(result.parseOptionalUint(values, "Rpl_semi_sync_master_clients", 32))
	}
}

// parseOptionalUint returns zero for a missing value and records malformed
// values as soft errors.
func (result *FullStatusResult) parseOptionalUint(values map[string]string, name string, bitSize int) uint64 {
	value, ok := values[name]
	if !ok {
		return 0
	}
	parsed, err := strconv.ParseUint(value, 10, bitSize)
	if err != nil {
		result.SoftErrors = append(result.SoftErrors, vterrors.Wrapf(err, "failed to parse %s", name))
		return 0
	}
	return parsed
}

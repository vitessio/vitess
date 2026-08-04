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

/*
Handle creating replicas and setting up the replication streams.
*/

package mysqlctl

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"strconv"
	"strings"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/hook"
	"vitess.io/vitess/go/vt/log"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	// Queries used for RPCs
	getGlobalStatusQuery           = "SELECT variable_name, variable_value FROM performance_schema.global_status"
	fullStatusGlobalVariablesQuery = "SELECT variable_name, variable_value FROM performance_schema.global_variables WHERE variable_name IN %a"
	fullStatusGlobalStatusQuery    = "SELECT variable_name, variable_value FROM performance_schema.global_status WHERE variable_name IN %a"

	// superReadOnlyResetTimeout bounds the reset function returned by
	// SetSuperReadOnly.
	superReadOnlyResetTimeout = 1 * time.Minute
)

type (
	ResetSuperReadOnlyFunc func() error

	// FullStatusResult contains the FullStatus proto assembled by
	// TryCollectFullStatusData along with any non-fatal errors collected while
	// reading semi-sync data.
	FullStatusResult struct {
		Status     *replicationdatapb.FullStatus
		SoftErrors []error
	}

	// SetSuperReadOnlyOption configures how SetSuperReadOnly runs.
	SetSuperReadOnlyOption func(*setSuperReadOnlyOptions)

	setSuperReadOnlyOptions struct {
		lockWaitTimeout time.Duration
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

// WithLockWaitTimeout sets the session lock_wait_timeout (rounded up to whole
// seconds) for the SET GLOBAL super_read_only statement, bounding how long it
// waits for metadata locks held by in-flight queries. By default the server's
// value is left untouched. A zero or negative timeout is the same as omitting
// the option: the server's value is left untouched and the wait is unbounded.
func WithLockWaitTimeout(timeout time.Duration) SetSuperReadOnlyOption {
	return func(options *setSuperReadOnlyOptions) {
		options.lockWaitTimeout = timeout
	}
}

// WaitForReplicationStart waits until the deadline for replication to start.
// This validates the current primary is correct and can be connected to.
func WaitForReplicationStart(ctx context.Context, mysqld MysqlDaemon, replicaStartDeadline int) (err error) {
	var replicaStatus replication.ReplicationStatus
	for range replicaStartDeadline {
		replicaStatus, err = mysqld.ReplicationStatus(ctx)
		if err != nil {
			return err
		}

		if replicaStatus.Running() {
			return nil
		}
		time.Sleep(time.Second)
	}
	errs := make([]string, 0, 2)
	if replicaStatus.LastSQLError != "" {
		errs = append(errs, "Last_SQL_Error: "+replicaStatus.LastSQLError)
	}
	if replicaStatus.LastIOError != "" {
		errs = append(errs, "Last_IO_Error: "+replicaStatus.LastIOError)
	}

	if len(errs) != 0 {
		return errors.New(strings.Join(errs, ", "))
	}
	return nil
}

// StartReplication starts replication.
func (mysqld *Mysqld) StartReplication(ctx context.Context, hookExtraEnv map[string]string) error {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	if err := mysqld.executeSuperQueryListConn(ctx, conn, []string{conn.Conn.StartReplicationCommand()}); err != nil {
		return err
	}

	h := hook.NewSimpleHook("postflight_start_slave")
	h.ExtraEnv = hookExtraEnv
	return h.ExecuteOptional()
}

// StartReplicationUntilAfter starts replication until replication has come to `targetPos`, then it stops replication
func (mysqld *Mysqld) StartReplicationUntilAfter(ctx context.Context, targetPos replication.Position) error {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	queries := []string{conn.Conn.StartReplicationUntilAfterCommand(targetPos)}

	return mysqld.executeSuperQueryListConn(ctx, conn, queries)
}

// StartSQLThreadUntilAfter starts replication's SQL thread(s) until replication has come to `targetPos`, then it stops it
func (mysqld *Mysqld) StartSQLThreadUntilAfter(ctx context.Context, targetPos replication.Position) error {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	queries := []string{conn.Conn.StartSQLThreadUntilAfterCommand(targetPos)}

	return mysqld.executeSuperQueryListConn(ctx, conn, queries)
}

// StopReplication stops replication.
func (mysqld *Mysqld) StopReplication(ctx context.Context, hookExtraEnv map[string]string) error {
	h := hook.NewSimpleHook("preflight_stop_slave")
	h.ExtraEnv = hookExtraEnv
	if err := h.ExecuteOptional(); err != nil {
		return err
	}
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	return mysqld.executeSuperQueryListConn(ctx, conn, []string{conn.Conn.StopReplicationCommand()})
}

// StopIOThread stops a replica's IO thread only.
func (mysqld *Mysqld) StopIOThread(ctx context.Context) error {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	return mysqld.executeSuperQueryListConn(ctx, conn, []string{conn.Conn.StopIOThreadCommand()})
}

// StopSQLThread stops a replica's SQL thread(s) only.
func (mysqld *Mysqld) StopSQLThread(ctx context.Context) error {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	return mysqld.executeSuperQueryListConn(ctx, conn, []string{conn.Conn.StopSQLThreadCommand()})
}

// RestartReplication stops, resets and starts replication.
func (mysqld *Mysqld) RestartReplication(ctx context.Context, hookExtraEnv map[string]string) error {
	h := hook.NewSimpleHook("preflight_stop_slave")
	h.ExtraEnv = hookExtraEnv
	if err := h.ExecuteOptional(); err != nil {
		return err
	}
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	if err := mysqld.executeSuperQueryListConn(ctx, conn, conn.Conn.RestartReplicationCommands()); err != nil {
		return err
	}

	h = hook.NewSimpleHook("postflight_start_slave")
	h.ExtraEnv = hookExtraEnv
	return h.ExecuteOptional()
}

// GetMysqlPort returns mysql port
func (mysqld *Mysqld) GetMysqlPort(ctx context.Context) (int32, error) {
	// We can not use the connection pool here. This check runs very early
	// during MySQL startup when we still might be loading things like grants.
	// This means we need to use an isolated connection to avoid poisoning the
	// DBA connection pool for further queries.
	params, err := mysqld.dbcfgs.DbaConnector().MysqlParams()
	if err != nil {
		return 0, err
	}
	conn, err := mysql.Connect(ctx, params)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	qr, err := conn.ExecuteFetch("SHOW VARIABLES LIKE 'port'", 1, false)
	if err != nil {
		return 0, err
	}
	if len(qr.Rows) != 1 {
		return 0, errors.New("no port variable in mysql")
	}
	utemp, err := qr.Rows[0][1].ToCastUint64()
	if err != nil {
		return 0, err
	}
	return int32(utemp), nil
}

// GetServerID returns mysql server id
func (mysqld *Mysqld) GetServerID(ctx context.Context) (uint32, error) {
	qr, err := mysqld.FetchSuperQuery(ctx, "select @@global.server_id")
	if err != nil {
		return 0, err
	}
	if len(qr.Rows) != 1 {
		return 0, errors.New("no server_id in mysql")
	}
	utemp, err := qr.Rows[0][0].ToCastUint64()
	if err != nil {
		return 0, err
	}
	return uint32(utemp), nil
}

// GetServerUUID returns mysql server uuid
func (mysqld *Mysqld) GetServerUUID(ctx context.Context) (string, error) {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return "", err
	}
	defer conn.Recycle()

	return conn.Conn.GetServerUUID()
}

// GetGlobalStatusVars returns the server's global status variables asked for.
// An empty/nil variable name parameter slice means you want all of them.
func (mysqld *Mysqld) GetGlobalStatusVars(ctx context.Context, variables []string) (map[string]string, error) {
	query := getGlobalStatusQuery
	if len(variables) != 0 {
		// The format specifier is for any optional predicates.
		statusBv, err := sqltypes.BuildBindVariable(variables)
		if err != nil {
			return nil, err
		}
		query, err = sqlparser.ParseAndBind(
			getGlobalStatusQuery+" WHERE variable_name IN %a",
			statusBv,
		)
		if err != nil {
			return nil, err
		}
	}
	qr, err := mysqld.FetchSuperQuery(ctx, query)
	if err != nil {
		return nil, err
	}

	finalRes := make(map[string]string, len(qr.Rows))
	for _, row := range qr.Rows {
		if len(row) != 2 {
			return nil, vterrors.New(vtrpcpb.Code_INTERNAL, "incorrect number of fields in the row")
		}
		finalRes[row[0].ToString()] = row[1].ToString()
	}
	return finalRes, nil
}

// IsReadOnly return true if the instance is read only
func (mysqld *Mysqld) IsReadOnly(ctx context.Context) (bool, error) {
	qr, err := mysqld.FetchSuperQuery(ctx, "SHOW VARIABLES LIKE 'read_only'")
	if err != nil {
		return true, err
	}
	if len(qr.Rows) != 1 {
		return true, errors.New("no read_only variable in mysql")
	}
	if qr.Rows[0][1].ToString() == "ON" {
		return true, nil
	}
	return false, nil
}

// IsSuperReadOnly return true if the instance is super read only
func (mysqld *Mysqld) IsSuperReadOnly(ctx context.Context) (bool, error) {
	qr, err := mysqld.FetchSuperQuery(ctx, "SELECT @@global.super_read_only")
	if err != nil {
		return false, err
	}

	if len(qr.Rows) == 1 {
		sro := qr.Rows[0][0].ToString()
		if sro == "1" || sro == "ON" {
			return true, nil
		}
	}

	return false, nil
}

// SetReadOnly set/unset the read_only flag
func (mysqld *Mysqld) SetReadOnly(ctx context.Context, on bool) error {
	query := "SET GLOBAL read_only = "
	if on {
		query += "ON"
	} else {
		query += "OFF"
	}
	return mysqld.ExecuteSuperQuery(ctx, query)
}

// SetSuperReadOnly set/unset the super_read_only flag.
// Returns a function which is called to set super_read_only back to its original value.
func (mysqld *Mysqld) SetSuperReadOnly(ctx context.Context, on bool, opts ...SetSuperReadOnlyOption) (ResetSuperReadOnlyFunc, error) {
	var options setSuperReadOnlyOptions
	for _, opt := range opts {
		opt(&options)
	}

	superReadOnlyEnabled, err := mysqld.IsSuperReadOnly(ctx)
	if err != nil {
		return nil, err
	}

	// The reset function restores super_read_only to its original value, and
	// only exists when this call actually changes it. It can be used as a
	// defer by the caller.
	var resetFunc ResetSuperReadOnlyFunc
	if on != superReadOnlyEnabled {
		resetFunc = func() error {
			resetCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), superReadOnlyResetTimeout)
			defer cancel()
			return mysqld.execSetSuperReadOnly(resetCtx, superReadOnlyEnabled, setSuperReadOnlyOptions{})
		}
	}

	if err := mysqld.execSetSuperReadOnly(ctx, on, options); err != nil {
		return nil, err
	}

	return resetFunc, nil
}

// execSetSuperReadOnly runs the SET GLOBAL super_read_only statement, bounding
// how long it waits for metadata locks when options carries a lockWaitTimeout.
func (mysqld *Mysqld) execSetSuperReadOnly(ctx context.Context, on bool, options setSuperReadOnlyOptions) error {
	query := "SET GLOBAL super_read_only = "
	if on {
		query += "'ON'"
	} else {
		query += "'OFF'"
	}

	if options.lockWaitTimeout <= 0 {
		return mysqld.ExecuteSuperQuery(ctx, query)
	}

	// Pin a single connection so the session lock_wait_timeout applies to the
	// SET GLOBAL statement.
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	// lock_wait_timeout only supports whole seconds, so round up to keep
	// sub-second timeouts from truncating to 0.
	lockWaitTimeoutSeconds := int64(math.Ceil(options.lockWaitTimeout.Seconds()))
	setTimeoutQuery := fmt.Sprintf("SET SESSION lock_wait_timeout = %d", lockWaitTimeoutSeconds)
	if err := mysqld.executeSuperQueryListConn(ctx, conn, []string{setTimeoutQuery}); err != nil {
		// Some servers don't know lock_wait_timeout. Proceed without a
		// bound rather than return an error callers could mistake for
		// super_read_only being unknown.
		sqlErr, ok := errors.AsType[*sqlerror.SQLError](err)
		if !ok || sqlErr.Number() != sqlerror.ERUnknownSystemVariable {
			return err
		}

		log.Warn("server does not know about lock_wait_timeout, continuing without bounding the lock wait", slog.Any("error", err))

		return mysqld.executeSuperQueryListConn(ctx, conn, []string{query})
	}

	execErr := mysqld.executeSuperQueryListConn(ctx, conn, []string{query})
	if execErr != nil && ctx.Err() != nil {
		// The connection was interrupted mid-query, so it must not return to the pool.
		conn.Taint()
		return execErr
	}

	// Restore the session so the connection can return to the pool.
	restoreQuery := "SET SESSION lock_wait_timeout = @@global.lock_wait_timeout"
	if err := mysqld.executeSuperQueryListConn(ctx, conn, []string{restoreQuery}); err != nil {
		log.Warn("failed to restore the session lock_wait_timeout, discarding the connection", slog.Any("error", err))
		conn.Taint()
	}

	return execErr
}

// WaitSourcePos lets replicas wait for the given replication position to
// be reached.
func (mysqld *Mysqld) WaitSourcePos(ctx context.Context, targetPos replication.Position) error {
	// Get a connection.
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	// First check if filePos flavored Position was passed in. If so, we
	// can't defer to the flavor in the connection, unless that flavor is
	// also filePos.
	if targetPos.MatchesFlavor(replication.FilePosFlavorID) {
		// If we are the primary, WaitUntilFilePosition will fail. But
		// position is most likely reached. So, check the position first.
		mpos, err := conn.Conn.PrimaryFilePosition()
		if err != nil {
			return vterrors.Wrapf(err, "WaitSourcePos: PrimaryFilePosition failed")
		}
		if mpos.AtLeast(targetPos) {
			return nil
		}
	} else {
		// If we are the primary, WaitUntilPosition will fail. But
		// position is most likely reached. So, check the position first.
		mpos, err := conn.Conn.PrimaryPosition()
		if err != nil {
			return vterrors.Wrapf(err, "WaitSourcePos: PrimaryPosition failed")
		}
		if mpos.AtLeast(targetPos) {
			return nil
		}
	}

	if err := conn.Conn.WaitUntilPosition(ctx, targetPos); err != nil {
		return vterrors.Wrapf(err, "WaitSourcePos failed")
	}
	return nil
}

func (mysqld *Mysqld) CatchupToGTID(ctx context.Context, targetPos replication.Position) error {
	params, err := mysqld.dbcfgs.ReplConnector().MysqlParams()
	if err != nil {
		return err
	}
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	cmds := conn.Conn.CatchupToGTIDCommands(params, targetPos)
	return mysqld.executeSuperQueryListConn(ctx, conn, cmds)
}

// ReplicationStatus returns the server replication status
func (mysqld *Mysqld) ReplicationStatus(ctx context.Context) (replication.ReplicationStatus, error) {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return replication.ReplicationStatus{}, err
	}
	defer conn.Recycle()

	return conn.Conn.ShowReplicationStatus()
}

// PrimaryStatus returns the primary replication statuses
func (mysqld *Mysqld) PrimaryStatus(ctx context.Context) (replication.PrimaryStatus, error) {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return replication.PrimaryStatus{}, err
	}
	defer conn.Recycle()

	primaryStatus, err := conn.Conn.ShowPrimaryStatus()
	if err != nil {
		return replication.PrimaryStatus{}, err
	}
	primaryStatus.ServerUUID, err = conn.Conn.GetServerUUID()
	if err != nil {
		return replication.PrimaryStatus{}, err
	}
	return primaryStatus, nil
}

func (mysqld *Mysqld) ReplicationConfiguration(ctx context.Context) (*replicationdatapb.Configuration, error) {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()

	return conn.Conn.ReplicationConfiguration()
}

// TryCollectFullStatusData collects FullStatus data on one connection for
// supported MySQL flavors. A nil result (with a nil error) means the caller
// should use the existing collection path.
func (mysqld *Mysqld) TryCollectFullStatusData(ctx context.Context) (*FullStatusResult, error) {
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

	err = runFullStatusQuery(ctx, conn, "replication configuration", func() error {
		var queryErr error
		status.ReplicationConfiguration, queryErr = conn.Conn.ReplicationConfiguration()
		return queryErr
	})
	if err != nil {
		return nil, err
	}

	if err := mysqld.collectFullStatusSemiSync(ctx, conn, result); err != nil {
		return nil, err
	}

	return result, nil
}

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

func (mysqld *Mysqld) fetchFullStatusVariables(ctx context.Context, conn *dbconnpool.PooledDBConnection) (*mysql.FullStatusVariables, error) {
	qr, err := mysqld.executeFetchContext(ctx, conn, conn.Conn.FullStatusVariablesQuery(), 1, true)
	if err != nil {
		return nil, err
	}
	return mysql.ParseFullStatusVariables(qr)
}

func (mysqld *Mysqld) collectFullStatusSemiSync(ctx context.Context, conn *dbconnpool.PooledDBConnection, result *FullStatusResult) error {
	variables, err := mysqld.fetchFullStatusValues(ctx, conn, fullStatusGlobalVariablesQuery, fullStatusSemiSyncVariables)
	if err != nil {
		result.SoftErrors = append(result.SoftErrors, vterrors.Wrapf(err, "failed to read semi-sync variables"))
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err = conn.Conn.Reconnect(ctx); err != nil {
			result.SoftErrors = append(result.SoftErrors, vterrors.Wrapf(err, "failed to reconnect before reading semi-sync status"))
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return nil
		}
	} else {
		result.parseSemiSyncVariables(variables)
	}

	statuses, err := mysqld.fetchFullStatusValues(ctx, conn, fullStatusGlobalStatusQuery, fullStatusSemiSyncStatuses)
	if err != nil {
		result.SoftErrors = append(result.SoftErrors, vterrors.Wrapf(err, "failed to read semi-sync status"))
		if ctx.Err() != nil {
			return ctx.Err()
		}
	} else {
		result.parseSemiSyncStatuses(statuses)
	}
	return nil
}

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

// GetGTIDPurged returns the gtid purged statuses
func (mysqld *Mysqld) GetGTIDPurged(ctx context.Context) (replication.Position, error) {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return replication.Position{}, err
	}
	defer conn.Recycle()

	return conn.Conn.GetGTIDPurged()
}

// PrimaryPosition returns the primary replication position.
func (mysqld *Mysqld) PrimaryPosition(ctx context.Context) (replication.Position, error) {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return replication.Position{}, err
	}
	defer conn.Recycle()

	return conn.Conn.PrimaryPosition()
}

// SetReplicationPosition sets the replication position at which the replica will resume
// when its replication is started.
func (mysqld *Mysqld) SetReplicationPosition(ctx context.Context, pos replication.Position) error {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	cmds := conn.Conn.SetReplicationPositionCommands(pos)
	log.Info(fmt.Sprintf("Executing commands to set replication position: %v", cmds))
	return mysqld.executeSuperQueryListConn(ctx, conn, cmds)
}

// SetReplicationSource makes the provided host / port the primary. It optionally
// stops replication before, and starts it after.
func (mysqld *Mysqld) SetReplicationSource(ctx context.Context, host string, port int32, heartbeatInterval float64, stopReplicationBefore bool, startReplicationAfter bool) error {
	params, err := mysqld.dbcfgs.ReplConnector().MysqlParams()
	if err != nil {
		return err
	}
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return err
	}
	defer conn.Recycle()

	var cmds []string
	if stopReplicationBefore {
		cmds = append(cmds, conn.Conn.StopReplicationCommand())
	}
	smc := conn.Conn.SetReplicationSourceCommand(params, host, port, heartbeatInterval, int(replicationConnectRetry.Seconds()))
	cmds = append(cmds, smc)
	if startReplicationAfter {
		cmds = append(cmds, conn.Conn.StartReplicationCommand())
	}
	return mysqld.executeSuperQueryListConn(ctx, conn, cmds)
}

// ResetReplication resets all replication for this host.
func (mysqld *Mysqld) ResetReplication(ctx context.Context) error {
	conn, connErr := getPoolReconnect(ctx, mysqld.dbaPool)
	if connErr != nil {
		return connErr
	}
	defer conn.Recycle()

	cmds := conn.Conn.ResetReplicationCommands()
	return mysqld.executeSuperQueryListConn(ctx, conn, cmds)
}

// ResetReplicationParameters resets the replica replication parameters for this host.
func (mysqld *Mysqld) ResetReplicationParameters(ctx context.Context) error {
	conn, connErr := getPoolReconnect(ctx, mysqld.dbaPool)
	if connErr != nil {
		return connErr
	}
	defer conn.Recycle()

	cmds := conn.Conn.ResetReplicationParametersCommands()
	return mysqld.executeSuperQueryListConn(ctx, conn, cmds)
}

// +------+---------+---------------------+------+-------------+------+------------------------------------------------------------------+------------------+
// | Id   | User    | Host                | db   | Command     | Time | State                                                            | Info             |
// +------+---------+---------------------+------+-------------+------+------------------------------------------------------------------+------------------+
// | 9792 | vt_repl | host:port           | NULL | Binlog Dump |   54 | Has sent all binlog to replica; waiting for binlog to be updated | NULL             |
// | 9797 | vt_dba  | localhost           | NULL | Query       |    0 | NULL                                                             | show processlist |
// +------+---------+---------------------+------+-------------+------+------------------------------------------------------------------+------------------+
//
// Array indices for the results of SHOW PROCESSLIST.
const (
	colConnectionID = iota
	colUsername
	colClientAddr
	colDbName
	colCommand
)

const (
	// this is the command used by mysql replicas
	binlogDumpCommand = "Binlog Dump"
)

// FindReplicas gets IP addresses for all currently connected replicas.
func FindReplicas(ctx context.Context, mysqld MysqlDaemon) ([]string, error) {
	qr, err := mysqld.FetchSuperQuery(ctx, "SHOW PROCESSLIST")
	if err != nil {
		return nil, err
	}
	addrs := make([]string, 0, 32)
	for _, row := range qr.Rows {
		// Check for prefix, since it could be "Binlog Dump GTID".
		if strings.HasPrefix(row[colCommand].ToString(), binlogDumpCommand) {
			host := row[colClientAddr].ToString()
			if host == "localhost" {
				// If we have a local binlog streamer, it will
				// show up as being connected
				// from 'localhost' through the local
				// socket. Ignore it.
				continue
			}
			host, _, err = netutil.SplitHostPort(host)
			if err != nil {
				return nil, fmt.Errorf("FindReplicas: malformed addr %v", err)
			}
			var ips []string
			ips, err = net.LookupHost(host)
			if err != nil {
				return nil, fmt.Errorf("FindReplicas: LookupHost failed %v", err)
			}
			addrs = append(addrs, ips...)
		}
	}

	return addrs, nil
}

// GetBinlogInformation gets the binlog format, whether binlog is enabled and if updates on replica logging is enabled.
func (mysqld *Mysqld) GetBinlogInformation(ctx context.Context) (string, bool, bool, string, error) {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return "", false, false, "", err
	}
	defer conn.Recycle()

	return conn.Conn.BinlogInformation()
}

// GetGTIDMode gets the GTID mode for the server
func (mysqld *Mysqld) GetGTIDMode(ctx context.Context) (string, error) {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return "", err
	}
	defer conn.Recycle()

	return conn.Conn.GetGTIDMode()
}

// FlushBinaryLogs is part of the MysqlDaemon interface.
func (mysqld *Mysqld) FlushBinaryLogs(ctx context.Context) (err error) {
	_, err = mysqld.FetchSuperQuery(ctx, "FLUSH BINARY LOGS")
	return err
}

// GetBinaryLogs is part of the MysqlDaemon interface.
func (mysqld *Mysqld) GetBinaryLogs(ctx context.Context) (binaryLogs []string, err error) {
	qr, err := mysqld.FetchSuperQuery(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return binaryLogs, err
	}
	for _, row := range qr.Rows {
		binaryLogs = append(binaryLogs, row[0].ToString())
	}
	return binaryLogs, err
}

// GetPreviousGTIDs is part of the MysqlDaemon interface.
func (mysqld *Mysqld) GetPreviousGTIDs(ctx context.Context, binlog string) (previousGtids string, err error) {
	query := fmt.Sprintf("SHOW BINLOG EVENTS IN '%s' LIMIT 2", binlog)
	qr, err := mysqld.FetchSuperQuery(ctx, query)
	if err != nil {
		return previousGtids, err
	}
	previousGtidsFound := false
	for _, row := range qr.Named().Rows {
		if row.AsString("Event_type", "") == "Previous_gtids" {
			previousGtids = row.AsString("Info", "")
			previousGtidsFound = true
		}
	}
	if !previousGtidsFound {
		return previousGtids, errors.New("GetPreviousGTIDs: previous GTIDs not found")
	}
	return previousGtids, nil
}

var ErrNoSemiSync = errors.New("semi-sync plugin not loaded")

func (mysqld *Mysqld) SemiSyncType(ctx context.Context) mysql.SemiSyncType {
	if mysqld.semiSyncType == mysql.SemiSyncTypeUnknown {
		mysqld.semiSyncType, _ = mysqld.SemiSyncExtensionLoaded(ctx)
	}
	return mysqld.semiSyncType
}

func (mysqld *Mysqld) enableSemiSyncQuery(ctx context.Context) (string, error) {
	switch mysqld.SemiSyncType(ctx) {
	case mysql.SemiSyncTypeSource:
		return "SET GLOBAL rpl_semi_sync_source_enabled = %v, GLOBAL rpl_semi_sync_replica_enabled = %v", nil
	case mysql.SemiSyncTypeMaster:
		return "SET GLOBAL rpl_semi_sync_master_enabled = %v, GLOBAL rpl_semi_sync_slave_enabled = %v", nil
	}
	return "", ErrNoSemiSync
}

func (mysqld *Mysqld) semiSyncClientsQuery(ctx context.Context) (string, error) {
	switch mysqld.SemiSyncType(ctx) {
	case mysql.SemiSyncTypeSource:
		return "SHOW STATUS LIKE 'Rpl_semi_sync_source_clients'", nil
	case mysql.SemiSyncTypeMaster:
		return "SHOW STATUS LIKE 'Rpl_semi_sync_master_clients'", nil
	}
	return "", ErrNoSemiSync
}

func (mysqld *Mysqld) semiSyncReplicationStatusQuery(ctx context.Context) (string, error) {
	switch mysqld.SemiSyncType(ctx) {
	case mysql.SemiSyncTypeSource:
		return "SHOW STATUS LIKE 'rpl_semi_sync_replica_status'", nil
	case mysql.SemiSyncTypeMaster:
		return "SHOW STATUS LIKE 'rpl_semi_sync_slave_status'", nil
	}
	return "", ErrNoSemiSync
}

// SetSemiSyncEnabled enables or disables semi-sync replication for
// primary and/or replica mode.
func (mysqld *Mysqld) SetSemiSyncEnabled(ctx context.Context, primary, replica bool) error {
	log.Info(fmt.Sprintf("Setting semi-sync mode: primary=%v, replica=%v", primary, replica))

	// Convert bool to int.
	var p, s int
	if primary {
		p = 1
	}
	if replica {
		s = 1
	}

	query, err := mysqld.enableSemiSyncQuery(ctx)
	if err != nil {
		return err
	}
	err = mysqld.ExecuteSuperQuery(ctx, fmt.Sprintf(query, p, s))
	if err != nil {
		return fmt.Errorf("can't set semi-sync mode: %v; make sure plugins are loaded in my.cnf", err)
	}
	return nil
}

// SemiSyncEnabled returns whether semi-sync is enabled for primary or replica.
// If the semi-sync plugin is not loaded, we assume semi-sync is disabled.
func (mysqld *Mysqld) SemiSyncEnabled(ctx context.Context) (primary, replica bool) {
	vars, err := mysqld.fetchVariables(ctx, "rpl_semi_sync_%_enabled")
	if err != nil {
		return false, false
	}
	switch mysqld.SemiSyncType(ctx) {
	case mysql.SemiSyncTypeSource:
		primary = vars["rpl_semi_sync_source_enabled"] == "ON"
		replica = vars["rpl_semi_sync_replica_enabled"] == "ON"
	case mysql.SemiSyncTypeMaster:
		primary = vars["rpl_semi_sync_master_enabled"] == "ON"
		replica = vars["rpl_semi_sync_slave_enabled"] == "ON"
	}
	return primary, replica
}

// SemiSyncStatus returns the current status of semi-sync for primary and replica.
func (mysqld *Mysqld) SemiSyncStatus(ctx context.Context) (primary, replica bool) {
	vars, err := mysqld.fetchStatuses(ctx, "Rpl_semi_sync_%_status")
	if err != nil {
		return false, false
	}
	switch mysqld.SemiSyncType(ctx) {
	case mysql.SemiSyncTypeSource:
		primary = vars["Rpl_semi_sync_source_status"] == "ON"
		replica = vars["Rpl_semi_sync_replica_status"] == "ON"
	case mysql.SemiSyncTypeMaster:
		primary = vars["Rpl_semi_sync_master_status"] == "ON"
		replica = vars["Rpl_semi_sync_slave_status"] == "ON"
	}
	return primary, replica
}

// SemiSyncClients returns the number of semi-sync clients for the primary.
func (mysqld *Mysqld) SemiSyncClients(ctx context.Context) uint32 {
	query, err := mysqld.semiSyncClientsQuery(ctx)
	if err != nil {
		return 0
	}
	qr, err := mysqld.FetchSuperQuery(ctx, query)
	if err != nil {
		return 0
	}
	if len(qr.Rows) != 1 {
		return 0
	}
	countStr := qr.Rows[0][1].ToString()
	count, _ := strconv.ParseUint(countStr, 10, 32)
	return uint32(count)
}

// SemiSyncSettings returns the settings of semi-sync which includes the timeout and the number of replicas to wait for.
func (mysqld *Mysqld) SemiSyncSettings(ctx context.Context) (timeout uint64, numReplicas uint32) {
	vars, err := mysqld.fetchVariables(ctx, "rpl_semi_sync_%")
	if err != nil {
		return 0, 0
	}
	var numReplicasUint uint64
	switch mysqld.SemiSyncType(ctx) {
	case mysql.SemiSyncTypeSource:
		timeout, _ = strconv.ParseUint(vars["rpl_semi_sync_source_timeout"], 10, 64)
		numReplicasUint, _ = strconv.ParseUint(vars["rpl_semi_sync_source_wait_for_replica_count"], 10, 32)
	case mysql.SemiSyncTypeMaster:
		timeout, _ = strconv.ParseUint(vars["rpl_semi_sync_master_timeout"], 10, 64)
		numReplicasUint, _ = strconv.ParseUint(vars["rpl_semi_sync_master_wait_for_slave_count"], 10, 32)
	}
	return timeout, uint32(numReplicasUint)
}

// SemiSyncReplicationStatus returns whether semi-sync is currently used by replication.
func (mysqld *Mysqld) SemiSyncReplicationStatus(ctx context.Context) (bool, error) {
	query, err := mysqld.semiSyncReplicationStatusQuery(ctx)
	if err != nil {
		return false, err
	}
	qr, err := mysqld.FetchSuperQuery(ctx, query)
	if err != nil {
		return false, err
	}
	if len(qr.Rows) != 1 {
		return false, errors.New("no rpl_semi_sync_replica_status variable in mysql")
	}
	if qr.Rows[0][1].ToString() == "ON" {
		return true, nil
	}
	return false, nil
}

// SemiSyncExtensionLoaded returns whether semi-sync plugins are loaded.
func (mysqld *Mysqld) SemiSyncExtensionLoaded(ctx context.Context) (mysql.SemiSyncType, error) {
	conn, connErr := getPoolReconnect(ctx, mysqld.dbaPool)
	if connErr != nil {
		return mysql.SemiSyncTypeUnknown, connErr
	}
	defer conn.Recycle()

	return conn.Conn.SemiSyncExtensionLoaded()
}

func (mysqld *Mysqld) IsSemiSyncBlocked(ctx context.Context) (bool, error) {
	conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
	if err != nil {
		return false, err
	}
	defer conn.Recycle()

	// Execute the query to check if the primary is blocked on semi-sync.
	semiSyncWaitSessionsRead := "select variable_value from performance_schema.global_status where regexp_like(variable_name, 'Rpl_semi_sync_(source|master)_wait_sessions')"
	res, err := conn.Conn.ExecuteFetch(semiSyncWaitSessionsRead, 1, false)
	if err != nil {
		return false, err
	}
	// If we have no rows, then the primary doesn't have semi-sync enabled.
	// It then follows, that the primary isn't blocked :)
	if len(res.Rows) == 0 {
		return false, nil
	}

	// Read the status value and check if it is non-zero.
	if len(res.Rows) != 1 || len(res.Rows[0]) != 1 {
		return false, fmt.Errorf("unexpected number of rows received - %v", res.Rows)
	}
	value, err := res.Rows[0][0].ToCastInt64()
	return value != 0, err
}

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

package mysqlctl

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

// FakeMysqlDaemon implements MysqlDaemon and allows the user to fake
// everything.
type FakeMysqlDaemon struct {
	mu sync.Mutex
	// db is the fake SQL DB we may use for some queries.
	db *fakesqldb.DB

	// appPool is set if db is set.
	appPool *dbconnpool.ConnectionPool

	// Running is used by Start / Shutdown
	Running bool

	// StartupTime is used to simulate mysqlds that take some time to respond
	// to a "start" command. It is used by Start.
	StartupTime time.Duration

	// ShutdownTime is used to simulate mysqlds that take some time to respond
	// to a "stop" request (i.e. a wedged systemd unit). It is used by Shutdown.
	ShutdownTime time.Duration

	// MysqlPort will be returned by GetMysqlPort(). Set to -1 to
	// return an error.
	MysqlPort atomic.Int32

	// Replicating is updated when calling StartReplication / StopReplication
	// (it is not used at all when calling ReplicationStatus, it is the
	// test owner responsibility to have these two match)
	Replicating bool

	// IOThreadRunning is always true except in one testcase
	// where we want to test error handling during SetReplicationSource
	IOThreadRunning bool

	// CurrentPrimaryPosition is returned by PrimaryPosition
	// and ReplicationStatus
	CurrentPrimaryPosition mysql.Position

	// CurrentSourceFilePosition is used to determine the executed file based positioning of the replication source.
	CurrentSourceFilePosition mysql.Position

	// ReplicationStatusError is used by ReplicationStatus
	ReplicationStatusError error

	// StartReplicationError is used by StartReplication
	StartReplicationError error

	// PromoteLag is the time for which Promote will stall
	PromoteLag time.Duration

	// PrimaryStatusError is used by PrimaryStatus
	PrimaryStatusError error

	// CurrentSourceHost is returned by ReplicationStatus
	CurrentSourceHost string

	// CurrentSourcePort is returned by ReplicationStatus
	CurrentSourcePort int32

	// ReplicationLagSeconds is returned by ReplicationStatus
	ReplicationLagSeconds uint32

	// ReadOnly is the current value of the flag
	ReadOnly bool

	// SuperReadOnly is the current value of the flag
	SuperReadOnly atomic.Bool

	// SetReplicationPositionPos is matched against the input of SetReplicationPosition.
	// If it doesn't match, SetReplicationPosition will return an error.
	SetReplicationPositionPos mysql.Position

	// StartReplicationUntilAfterPos is matched against the input
	StartReplicationUntilAfterPos mysql.Position

	// SetReplicationSourceInputs are matched against the input of SetReplicationSource
	// (as "%v:%v"). If all of them don't match, SetReplicationSource will return an error.
	SetReplicationSourceInputs []string

	// SetReplicationSourceError is used by SetReplicationSource
	SetReplicationSourceError error

	// StopReplicationError error is used by StopReplication
	StopReplicationError error

	// WaitPrimaryPositions is checked by WaitSourcePos, if the value is found
	// in it, then the function returns nil, else the function returns an error
	WaitPrimaryPositions []mysql.Position

	// PromoteResult is returned by Promote
	PromoteResult mysql.Position

	// PromoteError is used by Promote
	PromoteError error

	// SchemaFunc provides the return value for GetSchema.
	// If not defined, the "Schema" field will be used instead, see below.
	SchemaFunc func() (*tabletmanagerdatapb.SchemaDefinition, error)

	// Schema will be returned by GetSchema. If nil we'll
	// return an error.
	Schema *tabletmanagerdatapb.SchemaDefinition

	// PreflightSchemaChangeResult will be returned by PreflightSchemaChange.
	// If nil we'll return an error.
	PreflightSchemaChangeResult []*tabletmanagerdatapb.SchemaChangeResult

	// ApplySchemaChangeResult will be returned by ApplySchemaChange.
	// If nil we'll return an error.
	ApplySchemaChangeResult *tabletmanagerdatapb.SchemaChangeResult

	// ExpectedExecuteSuperQueryList is what we expect
	// ExecuteSuperQueryList to be called with. If it doesn't
	// match, ExecuteSuperQueryList will return an error.
	// Note each string is just a substring if it begins with SUB,
	// so we support partial queries (useful when queries contain
	// data fields like timestamps)
	ExpectedExecuteSuperQueryList []string

	// ExpectedExecuteSuperQueryCurrent is the current index of the queries
	// we expect
	ExpectedExecuteSuperQueryCurrent int

	// FetchSuperQueryResults is used by FetchSuperQuery
	FetchSuperQueryMap map[string]*sqltypes.Result

	// SemiSyncPrimaryEnabled represents the state of rpl_semi_sync_master_enabled.
	SemiSyncPrimaryEnabled bool
	// SemiSyncReplicaEnabled represents the state of rpl_semi_sync_slave_enabled.
	SemiSyncReplicaEnabled bool

	// TimeoutHook is a func that can be called at the beginning of any method to fake a timeout.
	// all a test needs to do is make it { return context.DeadlineExceeded }
	TimeoutHook func() error

	// Version is the version that will be returned by GetVersionString.
	Version string
}

// NewFakeMysqlDaemon returns a FakeMysqlDaemon where mysqld appears
// to be running, based on a fakesqldb.DB.
// 'db' can be nil if the test doesn't use a database at all.
func NewFakeMysqlDaemon(db *fakesqldb.DB) *FakeMysqlDaemon {
	result := &FakeMysqlDaemon{
		db:              db,
		Running:         true,
		IOThreadRunning: true,
		Version:         "8.0.32",
	}
	if db != nil {
		result.appPool = dbconnpool.NewConnectionPool("AppConnPool", 5, time.Minute, 0, 0)
		result.appPool.Open(db.ConnParams())
	}
	return result
}

// Start is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) Start(ctx context.Context, cnf *Mycnf, mysqldArgs ...string) error {
	if fmd.Running {
		return fmt.Errorf("fake mysql daemon already running")
	}

	if fmd.StartupTime > 0 {
		select {
		case <-time.After(fmd.StartupTime):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	fmd.Running = true
	return nil
}

// Shutdown is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) Shutdown(ctx context.Context, cnf *Mycnf, waitForMysqld bool) error {
	if !fmd.Running {
		return fmt.Errorf("fake mysql daemon not running")
	}

	if fmd.ShutdownTime > 0 {
		select {
		case <-time.After(fmd.ShutdownTime):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	fmd.Running = false
	return nil
}

// RunMysqlUpgrade is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) RunMysqlUpgrade(ctx context.Context) error {
	return nil
}

// ApplyBinlogFile is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) ApplyBinlogFile(ctx context.Context, binlogFile string, restorePos mysql.Position) error {
	return nil
}

// ReinitConfig is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) ReinitConfig(ctx context.Context, cnf *Mycnf) error {
	return nil
}

// RefreshConfig is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) RefreshConfig(ctx context.Context, cnf *Mycnf) error {
	return nil
}

// Wait is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) Wait(ctx context.Context, cnf *Mycnf) error {
	return nil
}

// GetMysqlPort is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) GetMysqlPort() (int32, error) {
	if fmd.MysqlPort.Load() == -1 {
		return 0, fmt.Errorf("FakeMysqlDaemon.GetMysqlPort returns an error")
	}
	return fmd.MysqlPort.Load(), nil
}

// GetServerID is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) GetServerID(ctx context.Context) (uint32, error) {
	return 1, nil
}

// GetServerUUID is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) GetServerUUID(ctx context.Context) (string, error) {
	return "000000", nil
}

// CurrentPrimaryPositionLocked is thread-safe
func (fmd *FakeMysqlDaemon) CurrentPrimaryPositionLocked(pos mysql.Position) {
	fmd.mu.Lock()
	defer fmd.mu.Unlock()
	fmd.CurrentPrimaryPosition = pos
}

// ReplicationStatus is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) ReplicationStatus() (mysql.ReplicationStatus, error) {
	if fmd.ReplicationStatusError != nil {
		return mysql.ReplicationStatus{}, fmd.ReplicationStatusError
	}
	fmd.mu.Lock()
	defer fmd.mu.Unlock()
	return mysql.ReplicationStatus{
		Position:                               fmd.CurrentPrimaryPosition,
		FilePosition:                           fmd.CurrentSourceFilePosition,
		RelayLogSourceBinlogEquivalentPosition: fmd.CurrentSourceFilePosition,
		ReplicationLagSeconds:                  fmd.ReplicationLagSeconds,
		// implemented as AND to avoid changing all tests that were
		// previously using Replicating = false
		IOState:    mysql.ReplicationStatusToState(fmt.Sprintf("%v", fmd.Replicating && fmd.IOThreadRunning)),
		SQLState:   mysql.ReplicationStatusToState(fmt.Sprintf("%v", fmd.Replicating)),
		SourceHost: fmd.CurrentSourceHost,
		SourcePort: fmd.CurrentSourcePort,
	}, nil
}

// PrimaryStatus is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) PrimaryStatus(ctx context.Context) (mysql.PrimaryStatus, error) {
	if fmd.PrimaryStatusError != nil {
		return mysql.PrimaryStatus{}, fmd.PrimaryStatusError
	}
	return mysql.PrimaryStatus{
		Position:     fmd.CurrentPrimaryPosition,
		FilePosition: fmd.CurrentSourceFilePosition,
	}, nil
}

// GetGTIDPurged is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) GetGTIDPurged(ctx context.Context) (mysql.Position, error) {
	return mysql.Position{}, nil
}

// ResetReplication is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) ResetReplication(ctx context.Context) error {
	return fmd.ExecuteSuperQueryList(ctx, []string{
		"FAKE RESET ALL REPLICATION",
	})
}

// ResetReplicationParameters is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) ResetReplicationParameters(ctx context.Context) error {
	return fmd.ExecuteSuperQueryList(ctx, []string{
		"FAKE RESET REPLICA ALL",
	})
}

// GetBinlogInformation is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) GetBinlogInformation(ctx context.Context) (binlogFormat string, logEnabled bool, logReplicaUpdate bool, binlogRowImage string, err error) {
	return "ROW", true, true, "FULL", fmd.ExecuteSuperQueryList(ctx, []string{
		"FAKE select @@global",
	})
}

// GetGTIDMode is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) GetGTIDMode(ctx context.Context) (gtidMode string, err error) {
	return "ON", fmd.ExecuteSuperQueryList(ctx, []string{
		"FAKE select @@global",
	})
}

// FlushBinaryLogs is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) FlushBinaryLogs(ctx context.Context) (err error) {
	return fmd.ExecuteSuperQueryList(ctx, []string{
		"FAKE FLUSH BINARY LOGS",
	})
}

// GetBinaryLogs is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) GetBinaryLogs(ctx context.Context) (binaryLogs []string, err error) {
	return []string{}, fmd.ExecuteSuperQueryList(ctx, []string{
		"FAKE SHOW BINARY LOGS",
	})
}

// GetPreviousGTIDs is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) GetPreviousGTIDs(ctx context.Context, binlog string) (previousGtids string, err error) {
	return "", fmd.ExecuteSuperQueryList(ctx, []string{
		fmt.Sprintf("FAKE SHOW BINLOG EVENTS IN '%s' LIMIT 2", binlog),
	})
}

// PrimaryPosition is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) PrimaryPosition() (mysql.Position, error) {
	return fmd.CurrentPrimaryPosition, nil
}

// IsReadOnly is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) IsReadOnly() (bool, error) {
	return fmd.ReadOnly, nil
}

// IsSuperReadOnly is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) IsSuperReadOnly() (bool, error) {
	return fmd.SuperReadOnly.Load(), nil
}

// SetReadOnly is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) SetReadOnly(on bool) error {
	fmd.ReadOnly = on
	return nil
}

// SetSuperReadOnly is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) SetSuperReadOnly(on bool) (ResetSuperReadOnlyFunc, error) {
	fmd.SuperReadOnly.Store(on)
	fmd.ReadOnly = on
	return nil, nil
}

// StartReplication is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) StartReplication(hookExtraEnv map[string]string) error {
	if fmd.StartReplicationError != nil {
		return fmd.StartReplicationError
	}
	return fmd.ExecuteSuperQueryList(context.Background(), []string{
		"START SLAVE",
	})
}

// RestartReplication is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) RestartReplication(hookExtraEnv map[string]string) error {
	return fmd.ExecuteSuperQueryList(context.Background(), []string{
		"STOP SLAVE",
		"RESET SLAVE",
		"START SLAVE",
	})
}

// StartReplicationUntilAfter is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) StartReplicationUntilAfter(ctx context.Context, pos mysql.Position) error {
	if !reflect.DeepEqual(fmd.StartReplicationUntilAfterPos, pos) {
		return fmt.Errorf("wrong pos for StartReplicationUntilAfter: expected %v got %v", fmd.SetReplicationPositionPos, pos)
	}

	return fmd.ExecuteSuperQueryList(context.Background(), []string{
		"START SLAVE UNTIL AFTER",
	})
}

// StopReplication is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) StopReplication(hookExtraEnv map[string]string) error {
	if fmd.StopReplicationError != nil {
		return fmd.StopReplicationError
	}
	return fmd.ExecuteSuperQueryList(context.Background(), []string{
		"STOP SLAVE",
	})
}

// StopIOThread is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) StopIOThread(ctx context.Context) error {
	return fmd.ExecuteSuperQueryList(context.Background(), []string{
		"STOP SLAVE IO_THREAD",
	})
}

// SetReplicationPosition is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) SetReplicationPosition(ctx context.Context, pos mysql.Position) error {
	if !reflect.DeepEqual(fmd.SetReplicationPositionPos, pos) {
		return fmt.Errorf("wrong pos for SetReplicationPosition: expected %v got %v", fmd.SetReplicationPositionPos, pos)
	}
	return fmd.ExecuteSuperQueryList(ctx, []string{
		"FAKE SET SLAVE POSITION",
	})
}

// SetReplicationSource is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) SetReplicationSource(ctx context.Context, host string, port int32, stopReplicationBefore bool, startReplicationAfter bool) error {
	input := fmt.Sprintf("%v:%v", host, port)
	found := false
	for _, sourceInput := range fmd.SetReplicationSourceInputs {
		if sourceInput == input {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("wrong input for SetReplicationSourceCommands: expected a value in %v got %v", fmd.SetReplicationSourceInputs, input)
	}
	if fmd.SetReplicationSourceError != nil {
		return fmd.SetReplicationSourceError
	}
	cmds := []string{}
	if stopReplicationBefore {
		cmds = append(cmds, "STOP SLAVE")
	}
	cmds = append(cmds, "FAKE SET MASTER")
	if startReplicationAfter {
		cmds = append(cmds, "START SLAVE")
	}
	fmd.CurrentSourceHost = host
	fmd.CurrentSourcePort = port
	return fmd.ExecuteSuperQueryList(ctx, cmds)
}

// WaitForReparentJournal is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) WaitForReparentJournal(ctx context.Context, timeCreatedNS int64) error {
	return nil
}

// WaitSourcePos is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) WaitSourcePos(_ context.Context, pos mysql.Position) error {
	if fmd.TimeoutHook != nil {
		return fmd.TimeoutHook()
	}
	for _, position := range fmd.WaitPrimaryPositions {
		if reflect.DeepEqual(position, pos) {
			return nil
		}
	}
	return fmt.Errorf("wrong input for WaitSourcePos: expected a value in %v got %v", fmd.WaitPrimaryPositions, pos)
}

// Promote is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) Promote(hookExtraEnv map[string]string) (mysql.Position, error) {
	if fmd.PromoteLag > 0 {
		time.Sleep(fmd.PromoteLag)
	}
	if fmd.PromoteError != nil {
		return mysql.Position{}, fmd.PromoteError
	}
	return fmd.PromoteResult, nil
}

// ExecuteSuperQueryList is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) ExecuteSuperQueryList(ctx context.Context, queryList []string) error {
	for _, query := range queryList {
		// test we still have a query to compare
		if fmd.ExpectedExecuteSuperQueryCurrent >= len(fmd.ExpectedExecuteSuperQueryList) {
			return fmt.Errorf("unexpected extra query in ExecuteSuperQueryList: %v", query)
		}

		// compare the query
		expected := fmd.ExpectedExecuteSuperQueryList[fmd.ExpectedExecuteSuperQueryCurrent]
		fmd.ExpectedExecuteSuperQueryCurrent++
		if strings.HasPrefix(expected, "SUB") {
			// remove the SUB from the expected,
			// and truncate the query to length(expected)
			expected = expected[3:]
			if len(query) > len(expected) {
				query = query[:len(expected)]
			}
		}
		if expected != query {
			return fmt.Errorf("wrong query for ExecuteSuperQueryList: expected %v got %v", expected, query)
		}

		// intercept some queries to update our status
		switch query {
		case "START SLAVE":
			fmd.Replicating = true
		case "STOP SLAVE":
			fmd.Replicating = false
		}
	}
	return nil
}

// FetchSuperQuery returns the results from the map, if any
func (fmd *FakeMysqlDaemon) FetchSuperQuery(ctx context.Context, query string) (*sqltypes.Result, error) {
	if fmd.FetchSuperQueryMap == nil {
		return nil, fmt.Errorf("unexpected query: %v", query)
	}

	if qr, ok := fmd.FetchSuperQueryMap[query]; ok {
		return qr, nil
	}
	for k, qr := range fmd.FetchSuperQueryMap {
		if ok, _ := regexp.MatchString(k, query); ok {
			return qr, nil
		}
	}
	return nil, fmt.Errorf("unexpected query: %v", query)
}

// Close is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) Close() {
	if fmd.appPool != nil {
		fmd.appPool.Close()
	}
}

// CheckSuperQueryList returns an error if all the queries we expected
// haven't been seen.
func (fmd *FakeMysqlDaemon) CheckSuperQueryList() error {
	if fmd.ExpectedExecuteSuperQueryCurrent != len(fmd.ExpectedExecuteSuperQueryList) {
		return fmt.Errorf("SuperQueryList wasn't consumed, saw %v queries, was expecting %v", fmd.ExpectedExecuteSuperQueryCurrent, len(fmd.ExpectedExecuteSuperQueryList))
	}
	return nil
}

// GetSchema is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) GetSchema(ctx context.Context, dbName string, request *tabletmanagerdatapb.GetSchemaRequest) (*tabletmanagerdatapb.SchemaDefinition, error) {
	if fmd.SchemaFunc != nil {
		return fmd.SchemaFunc()
	}
	if fmd.Schema == nil {
		return nil, fmt.Errorf("no schema defined")
	}
	return tmutils.FilterTables(fmd.Schema, request.Tables, request.ExcludeTables, request.IncludeViews)
}

// GetColumns is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) GetColumns(ctx context.Context, dbName, table string) ([]*querypb.Field, []string, error) {
	return []*querypb.Field{}, []string{}, nil
}

// GetPrimaryKeyColumns is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) GetPrimaryKeyColumns(ctx context.Context, dbName, table string) ([]string, error) {
	return []string{}, nil
}

// GetPrimaryKeyEquivalentColumns is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) GetPrimaryKeyEquivalentColumns(ctx context.Context, dbName, table string) ([]string, error) {
	return []string{}, nil
}

// PreflightSchemaChange is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) PreflightSchemaChange(ctx context.Context, dbName string, changes []string) ([]*tabletmanagerdatapb.SchemaChangeResult, error) {
	if fmd.PreflightSchemaChangeResult == nil {
		return nil, fmt.Errorf("no preflight result defined")
	}
	return fmd.PreflightSchemaChangeResult, nil
}

// ApplySchemaChange is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) ApplySchemaChange(ctx context.Context, dbName string, change *tmutils.SchemaChange) (*tabletmanagerdatapb.SchemaChangeResult, error) {
	beforeSchema, err := fmd.SchemaFunc()
	if err != nil {
		return nil, err
	}

	dbaCon, err := fmd.GetDbaConnection(ctx)
	if err != nil {
		return nil, err
	}
	if err = fmd.db.HandleQuery(dbaCon.Conn, change.SQL, func(*sqltypes.Result) error { return nil }); err != nil {
		return nil, err
	}

	afterSchema, err := fmd.SchemaFunc()
	if err != nil {
		return nil, err
	}

	return &tabletmanagerdatapb.SchemaChangeResult{
		BeforeSchema: beforeSchema,
		AfterSchema:  afterSchema}, nil
}

// GetAppConnection is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) GetAppConnection(ctx context.Context) (*dbconnpool.PooledDBConnection, error) {
	return fmd.appPool.Get(ctx)
}

// GetDbaConnection is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) GetDbaConnection(ctx context.Context) (*dbconnpool.DBConnection, error) {
	return dbconnpool.NewDBConnection(ctx, fmd.db.ConnParams())
}

// GetAllPrivsConnection is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) GetAllPrivsConnection(ctx context.Context) (*dbconnpool.DBConnection, error) {
	return dbconnpool.NewDBConnection(ctx, fmd.db.ConnParams())
}

// SetSemiSyncEnabled is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) SetSemiSyncEnabled(primary, replica bool) error {
	fmd.SemiSyncPrimaryEnabled = primary
	fmd.SemiSyncReplicaEnabled = replica
	return nil
}

// SemiSyncEnabled is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) SemiSyncEnabled() (primary, replica bool) {
	return fmd.SemiSyncPrimaryEnabled, fmd.SemiSyncReplicaEnabled
}

// SemiSyncStatus is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) SemiSyncStatus() (bool, bool) {
	// The fake assumes the status worked.
	if fmd.SemiSyncPrimaryEnabled {
		return true, false
	}
	return false, fmd.SemiSyncReplicaEnabled
}

// SemiSyncClients is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) SemiSyncClients() uint32 {
	return 0
}

// SemiSyncExtensionLoaded is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) SemiSyncExtensionLoaded() (bool, error) {
	return true, nil
}

// SemiSyncSettings is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) SemiSyncSettings() (timeout uint64, numReplicas uint32) {
	return 10000000, 1
}

// SemiSyncReplicationStatus is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) SemiSyncReplicationStatus() (bool, error) {
	// The fake assumes the status worked.
	return fmd.SemiSyncReplicaEnabled, nil
}

// GetVersionString is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) GetVersionString(ctx context.Context) (string, error) {
	return fmd.Version, nil
}

// GetVersionComment is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) GetVersionComment(ctx context.Context) (string, error) {
	return "", nil
}

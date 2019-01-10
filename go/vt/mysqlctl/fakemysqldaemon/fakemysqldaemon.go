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

package fakemysqldaemon

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

// FakeMysqlDaemon implements MysqlDaemon and allows the user to fake
// everything.
type FakeMysqlDaemon struct {
	// db is the fake SQL DB we may use for some queries.
	db *fakesqldb.DB

	// appPool is set if db is set.
	appPool *dbconnpool.ConnectionPool

	// Running is used by Start / Shutdown
	Running bool

	// MysqlPort will be returned by GetMysqlPort(). Set to -1 to
	// return an error.
	MysqlPort int32

	// Replicating is updated when calling StartSlave / StopSlave
	// (it is not used at all when calling SlaveStatus, it is the
	// test owner responsability to have these two match)
	Replicating bool

	// CurrentMasterPosition is returned by MasterPosition
	// and SlaveStatus
	CurrentMasterPosition mysql.Position

	// SlaveStatusError is used by SlaveStatus
	SlaveStatusError error

	// CurrentMasterHost is returned by SlaveStatus
	CurrentMasterHost string

	// CurrentMasterport is returned by SlaveStatus
	CurrentMasterPort int

	// SecondsBehindMaster is returned by SlaveStatus
	SecondsBehindMaster uint

	// ReadOnly is the current value of the flag
	ReadOnly bool

	// SetSlavePositionPos is matched against the input of SetSlavePosition.
	// If it doesn't match, SetSlavePosition will return an error.
	SetSlavePositionPos mysql.Position

	// SetMasterInput is matched against the input of SetMaster
	// (as "%v:%v"). If it doesn't match, SetMaster will return an error.
	SetMasterInput string

	// DemoteMasterPosition is returned by DemoteMaster
	DemoteMasterPosition mysql.Position

	// WaitMasterPosition is checked by WaitMasterPos, if the
	// same it returns nil, if different it returns an error
	WaitMasterPosition mysql.Position

	// PromoteSlaveResult is returned by PromoteSlave
	PromoteSlaveResult mysql.Position

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
	// so we support partial queries (usefull when queries contain
	// data fields like timestamps)
	ExpectedExecuteSuperQueryList []string

	// ExpectedExecuteSuperQueryCurrent is the current index of the queries
	// we expect
	ExpectedExecuteSuperQueryCurrent int

	// FetchSuperQueryResults is used by FetchSuperQuery
	FetchSuperQueryMap map[string]*sqltypes.Result

	// BinlogPlayerEnabled is used by {Enable,Disable}BinlogPlayer
	BinlogPlayerEnabled sync2.AtomicBool

	// SemiSyncMasterEnabled represents the state of rpl_semi_sync_master_enabled.
	SemiSyncMasterEnabled bool
	// SemiSyncSlaveEnabled represents the state of rpl_semi_sync_slave_enabled.
	SemiSyncSlaveEnabled bool
}

// NewFakeMysqlDaemon returns a FakeMysqlDaemon where mysqld appears
// to be running, based on a fakesqldb.DB.
// 'db' can be nil if the test doesn't use a database at all.
func NewFakeMysqlDaemon(db *fakesqldb.DB) *FakeMysqlDaemon {
	result := &FakeMysqlDaemon{
		db:      db,
		Running: true,
	}
	if db != nil {
		result.appPool = dbconnpool.NewConnectionPool("AppConnPool", 5, time.Minute)
		result.appPool.Open(db.ConnParams(), stats.NewTimings("", "", ""))
	}
	return result
}

// Start is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) Start(ctx context.Context, cnf *mysqlctl.Mycnf, mysqldArgs ...string) error {
	if fmd.Running {
		return fmt.Errorf("fake mysql daemon already running")
	}
	fmd.Running = true
	return nil
}

// Shutdown is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) Shutdown(ctx context.Context, cnf *mysqlctl.Mycnf, waitForMysqld bool) error {
	if !fmd.Running {
		return fmt.Errorf("fake mysql daemon not running")
	}
	fmd.Running = false
	return nil
}

// RunMysqlUpgrade is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) RunMysqlUpgrade() error {
	return nil
}

// ReinitConfig is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) ReinitConfig(ctx context.Context, cnf *mysqlctl.Mycnf) error {
	return nil
}

// RefreshConfig is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) RefreshConfig(ctx context.Context, cnf *mysqlctl.Mycnf) error {
	return nil
}

// Wait is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) Wait(ctx context.Context, cnf *mysqlctl.Mycnf) error {
	return nil
}

// GetMysqlPort is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) GetMysqlPort() (int32, error) {
	if fmd.MysqlPort == -1 {
		return 0, fmt.Errorf("FakeMysqlDaemon.GetMysqlPort returns an error")
	}
	return fmd.MysqlPort, nil
}

// SlaveStatus is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) SlaveStatus() (mysql.SlaveStatus, error) {
	if fmd.SlaveStatusError != nil {
		return mysql.SlaveStatus{}, fmd.SlaveStatusError
	}
	return mysql.SlaveStatus{
		Position:            fmd.CurrentMasterPosition,
		SecondsBehindMaster: fmd.SecondsBehindMaster,
		SlaveIORunning:      fmd.Replicating,
		SlaveSQLRunning:     fmd.Replicating,
		MasterHost:          fmd.CurrentMasterHost,
		MasterPort:          fmd.CurrentMasterPort,
	}, nil
}

// ResetReplication is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) ResetReplication(ctx context.Context) error {
	return fmd.ExecuteSuperQueryList(ctx, []string{
		"FAKE RESET ALL REPLICATION",
	})
}

// MasterPosition is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) MasterPosition() (mysql.Position, error) {
	return fmd.CurrentMasterPosition, nil
}

// IsReadOnly is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) IsReadOnly() (bool, error) {
	return fmd.ReadOnly, nil
}

// SetReadOnly is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) SetReadOnly(on bool) error {
	fmd.ReadOnly = on
	return nil
}

// StartSlave is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) StartSlave(hookExtraEnv map[string]string) error {
	return fmd.ExecuteSuperQueryList(context.Background(), []string{
		"START SLAVE",
	})
}

// StopSlave is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) StopSlave(hookExtraEnv map[string]string) error {
	return fmd.ExecuteSuperQueryList(context.Background(), []string{
		"STOP SLAVE",
	})
}

// SetSlavePosition is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) SetSlavePosition(ctx context.Context, pos mysql.Position) error {
	if !reflect.DeepEqual(fmd.SetSlavePositionPos, pos) {
		return fmt.Errorf("wrong pos for SetSlavePosition: expected %v got %v", fmd.SetSlavePositionPos, pos)
	}
	return fmd.ExecuteSuperQueryList(ctx, []string{
		"FAKE SET SLAVE POSITION",
	})
}

// SetMaster is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) SetMaster(ctx context.Context, masterHost string, masterPort int, slaveStopBefore bool, slaveStartAfter bool) error {
	input := fmt.Sprintf("%v:%v", masterHost, masterPort)
	if fmd.SetMasterInput != input {
		return fmt.Errorf("wrong input for SetMasterCommands: expected %v got %v", fmd.SetMasterInput, input)
	}
	cmds := []string{}
	if slaveStopBefore {
		cmds = append(cmds, "STOP SLAVE")
	}
	cmds = append(cmds, "FAKE SET MASTER")
	if slaveStartAfter {
		cmds = append(cmds, "START SLAVE")
	}
	return fmd.ExecuteSuperQueryList(ctx, cmds)
}

// WaitForReparentJournal is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) WaitForReparentJournal(ctx context.Context, timeCreatedNS int64) error {
	return nil
}

// DemoteMaster is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) DemoteMaster() (mysql.Position, error) {
	return fmd.DemoteMasterPosition, nil
}

// WaitMasterPos is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) WaitMasterPos(_ context.Context, pos mysql.Position) error {
	if reflect.DeepEqual(fmd.WaitMasterPosition, pos) {
		return nil
	}
	return fmt.Errorf("wrong input for WaitMasterPos: expected %v got %v", fmd.WaitMasterPosition, pos)
}

// PromoteSlave is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) PromoteSlave(hookExtraEnv map[string]string) (mysql.Position, error) {
	return fmd.PromoteSlaveResult, nil
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

	qr, ok := fmd.FetchSuperQueryMap[query]
	if !ok {
		return nil, fmt.Errorf("unexpected query: %v", query)
	}
	return qr, nil
}

// EnableBinlogPlayback is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) EnableBinlogPlayback() error {
	fmd.BinlogPlayerEnabled.Set(true)
	return nil
}

// DisableBinlogPlayback disable playback of binlog events
func (fmd *FakeMysqlDaemon) DisableBinlogPlayback() error {
	fmd.BinlogPlayerEnabled.Set(false)
	return nil
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
func (fmd *FakeMysqlDaemon) GetSchema(dbName string, tables, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error) {
	if fmd.SchemaFunc != nil {
		return fmd.SchemaFunc()
	}
	if fmd.Schema == nil {
		return nil, fmt.Errorf("no schema defined")
	}
	return tmutils.FilterTables(fmd.Schema, tables, excludeTables, includeViews)
}

// GetPrimaryKeyColumns is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) GetPrimaryKeyColumns(dbName, table string) ([]string, error) {
	return []string{}, nil
}

// PreflightSchemaChange is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) PreflightSchemaChange(dbName string, changes []string) ([]*tabletmanagerdatapb.SchemaChangeResult, error) {
	if fmd.PreflightSchemaChangeResult == nil {
		return nil, fmt.Errorf("no preflight result defined")
	}
	return fmd.PreflightSchemaChangeResult, nil
}

// ApplySchemaChange is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) ApplySchemaChange(dbName string, change *tmutils.SchemaChange) (*tabletmanagerdatapb.SchemaChangeResult, error) {
	if fmd.ApplySchemaChangeResult == nil {
		return nil, fmt.Errorf("no apply schema defined")
	}
	return fmd.ApplySchemaChangeResult, nil
}

// GetAppConnection is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) GetAppConnection(ctx context.Context) (*dbconnpool.PooledDBConnection, error) {
	return fmd.appPool.Get(ctx)
}

// GetDbaConnection is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) GetDbaConnection() (*dbconnpool.DBConnection, error) {
	return dbconnpool.NewDBConnection(fmd.db.ConnParams(), stats.NewTimings("", "", ""))
}

// GetAllPrivsConnection is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) GetAllPrivsConnection() (*dbconnpool.DBConnection, error) {
	return dbconnpool.NewDBConnection(fmd.db.ConnParams(), stats.NewTimings("", "", ""))
}

// SetSemiSyncEnabled is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) SetSemiSyncEnabled(master, slave bool) error {
	fmd.SemiSyncMasterEnabled = master
	fmd.SemiSyncSlaveEnabled = slave
	return nil
}

// SemiSyncEnabled is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) SemiSyncEnabled() (master, slave bool) {
	return fmd.SemiSyncMasterEnabled, fmd.SemiSyncSlaveEnabled
}

// SemiSyncSlaveStatus is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) SemiSyncSlaveStatus() (bool, error) {
	// The fake assumes the status worked.
	return fmd.SemiSyncSlaveEnabled, nil
}

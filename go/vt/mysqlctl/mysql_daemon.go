// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"golang.org/x/net/context"
)

// MysqlDaemon is the interface we use for abstracting Mysqld.
type MysqlDaemon interface {
	// GetMasterAddr returns the mysql master address, as shown by
	// 'show slave status'.
	GetMasterAddr() (string, error)

	// GetMysqlPort returns the current port mysql is listening on.
	GetMysqlPort() (int, error)

	// replication related methods
	StartSlave(hookExtraEnv map[string]string) error
	StopSlave(hookExtraEnv map[string]string) error
	SlaveStatus() (*proto.ReplicationStatus, error)

	// reparenting related methods
	ResetReplicationCommands() ([]string, error)
	MasterPosition() (proto.ReplicationPosition, error)
	SetReadOnly(on bool) error
	StartReplicationCommands(status *proto.ReplicationStatus) ([]string, error)
	SetMasterCommands(masterHost string, masterPort int) ([]string, error)
	WaitForReparentJournal(ctx context.Context, timeCreatedNS int64) error

	// DemoteMaster waits for all current transactions to finish,
	// and returns the current replication position. It will not
	// change the read_only state of the server.
	DemoteMaster() (proto.ReplicationPosition, error)

	WaitMasterPos(proto.ReplicationPosition, time.Duration) error

	// PromoteSlave makes the slave the new master. It will not change
	// the read_only state of the server.
	PromoteSlave(map[string]string) (proto.ReplicationPosition, error)

	// Schema related methods
	GetSchema(dbName string, tables, excludeTables []string, includeViews bool) (*proto.SchemaDefinition, error)

	// GetAppConnection returns a app connection to be able to talk to the database.
	GetAppConnection() (dbconnpool.PoolConnection, error)
	// GetDbaConnection returns a dba connection.
	GetDbaConnection() (*dbconnpool.DBConnection, error)
	// query execution methods
	ExecuteSuperQueryList(queryList []string) error
}

// FakeMysqlDaemon implements MysqlDaemon and allows the user to fake
// everything.
type FakeMysqlDaemon struct {
	// MasterAddr will be returned by GetMasterAddr(). Set to "" to return
	// ErrNotSlave, or to "ERROR" to return an error.
	MasterAddr string

	// MysqlPort will be returned by GetMysqlPort(). Set to -1 to
	// return an error.
	MysqlPort int

	// Replicating is updated when calling StartSlave / StopSlave
	// (it is not used at all when calling SlaveStatus, it is the
	// test owner responsability to have these two match)
	Replicating bool

	// CurrentSlaveStatus is returned by SlaveStatus
	CurrentSlaveStatus *proto.ReplicationStatus

	// ResetReplicationResult is returned by ResetReplication
	ResetReplicationResult []string

	// ResetReplicationError is returned by ResetReplication
	ResetReplicationError error

	// CurrentMasterPosition is returned by MasterPosition
	CurrentMasterPosition proto.ReplicationPosition

	// ReadOnly is the current value of the flag
	ReadOnly bool

	// StartReplicationCommandsStatus is matched against the input
	// of StartReplicationCommands. If it doesn't match,
	// StartReplicationCommands will return an error.
	StartReplicationCommandsStatus *proto.ReplicationStatus

	// StartReplicationCommandsResult is what
	// StartReplicationCommands will return
	StartReplicationCommandsResult []string

	// SetMasterCommandsInput is matched against the input
	// of SetMasterCommands (as "%v:%v"). If it doesn't match,
	// SetMasterCommands will return an error.
	SetMasterCommandsInput string

	// SetMasterCommandsResult is what
	// SetMasterCommands will return
	SetMasterCommandsResult []string

	// DemoteMasterPosition is returned by DemoteMaster
	DemoteMasterPosition proto.ReplicationPosition

	// WaitMasterPosition is checked by WaitMasterPos, if the
	// same it returns nil, if different it returns an error
	WaitMasterPosition proto.ReplicationPosition

	// PromoteSlaveResult is returned by PromoteSlave
	PromoteSlaveResult proto.ReplicationPosition

	// Schema that will be returned by GetSchema. If nil we'll
	// return an error.
	Schema *proto.SchemaDefinition

	// DbaConnectionFactory is the factory for making fake dba connections
	DbaConnectionFactory func() (dbconnpool.PoolConnection, error)

	// DbAppConnectionFactory is the factory for making fake db app connections
	DbAppConnectionFactory func() (dbconnpool.PoolConnection, error)

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
}

// GetMasterAddr is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) GetMasterAddr() (string, error) {
	if fmd.MasterAddr == "" {
		return "", ErrNotSlave
	}
	if fmd.MasterAddr == "ERROR" {
		return "", fmt.Errorf("FakeMysqlDaemon.GetMasterAddr returns an error")
	}
	return fmd.MasterAddr, nil
}

// GetMysqlPort is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) GetMysqlPort() (int, error) {
	if fmd.MysqlPort == -1 {
		return 0, fmt.Errorf("FakeMysqlDaemon.GetMysqlPort returns an error")
	}
	return fmd.MysqlPort, nil
}

// StartSlave is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) StartSlave(hookExtraEnv map[string]string) error {
	fmd.Replicating = true
	return nil
}

// StopSlave is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) StopSlave(hookExtraEnv map[string]string) error {
	fmd.Replicating = false
	return nil
}

// SlaveStatus is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) SlaveStatus() (*proto.ReplicationStatus, error) {
	if fmd.CurrentSlaveStatus == nil {
		return nil, fmt.Errorf("no slave status defined")
	}
	return fmd.CurrentSlaveStatus, nil
}

// ResetReplicationCommands is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) ResetReplicationCommands() ([]string, error) {
	return fmd.ResetReplicationResult, fmd.ResetReplicationError
}

// MasterPosition is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) MasterPosition() (proto.ReplicationPosition, error) {
	return fmd.CurrentMasterPosition, nil
}

// SetReadOnly is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) SetReadOnly(on bool) error {
	fmd.ReadOnly = on
	return nil
}

// StartReplicationCommands is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) StartReplicationCommands(status *proto.ReplicationStatus) ([]string, error) {
	status.MasterConnectRetry = int(masterConnectRetry.Seconds())
	if !reflect.DeepEqual(fmd.StartReplicationCommandsStatus, status) {
		return nil, fmt.Errorf("wrong status for StartReplicationCommands: expected %v got %v", fmd.StartReplicationCommandsStatus, status)
	}
	return fmd.StartReplicationCommandsResult, nil
}

// SetMasterCommands is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) SetMasterCommands(masterHost string, masterPort int) ([]string, error) {
	input := fmt.Sprintf("%v:%v", masterHost, masterPort)
	if fmd.SetMasterCommandsInput != input {
		return nil, fmt.Errorf("wrong input for SetMasterCommands: expected %v got %v", fmd.SetMasterCommandsInput, input)
	}
	return fmd.SetMasterCommandsResult, nil
}

// WaitForReparentJournal is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) WaitForReparentJournal(ctx context.Context, timeCreatedNS int64) error {
	return nil
}

// DemoteMaster is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) DemoteMaster() (proto.ReplicationPosition, error) {
	return fmd.DemoteMasterPosition, nil
}

// WaitMasterPos is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) WaitMasterPos(pos proto.ReplicationPosition, waitTimeout time.Duration) error {
	if reflect.DeepEqual(fmd.WaitMasterPosition, pos) {
		return nil
	}
	return fmt.Errorf("wrong input for WaitMasterPos: expected %v got %v", fmd.WaitMasterPosition, pos)
}

// PromoteSlave is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) PromoteSlave(hookExtraEnv map[string]string) (proto.ReplicationPosition, error) {
	return fmd.PromoteSlaveResult, nil
}

// ExecuteSuperQueryList is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) ExecuteSuperQueryList(queryList []string) error {
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
	}
	return nil
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
func (fmd *FakeMysqlDaemon) GetSchema(dbName string, tables, excludeTables []string, includeViews bool) (*proto.SchemaDefinition, error) {
	if fmd.Schema == nil {
		return nil, fmt.Errorf("no schema defined")
	}
	return fmd.Schema.FilterTables(tables, excludeTables, includeViews)
}

// GetAppConnection is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) GetAppConnection() (dbconnpool.PoolConnection, error) {
	if fmd.DbAppConnectionFactory == nil {
		return nil, fmt.Errorf("no DbAppConnectionFactory set in this FakeMysqlDaemon")
	}
	return fmd.DbAppConnectionFactory()
}

// GetDbaConnection is part of the MysqlDaemon interface.
func (fmd *FakeMysqlDaemon) GetDbaConnection() (*dbconnpool.DBConnection, error) {
	return dbconnpool.NewDBConnection(&sqldb.ConnParams{}, stats.NewTimings(""))
}

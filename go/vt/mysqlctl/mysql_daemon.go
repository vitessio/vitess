// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
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

	// Schema related methods
	GetSchema(dbName string, tables, excludeTables []string, includeViews bool) (*proto.SchemaDefinition, error)

	// GetDbConnection returns a connection to be able to talk to the database.
	// It accepts a dbconfig name to determine which db user it the connection should have.
	GetDbConnection(dbconfigName dbconfigs.DbConfigName) (dbconnpool.PoolConnection, error)
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

	// Replicating is updated when calling StopSlave
	Replicating bool

	// CurrentSlaveStatus is returned by SlaveStatus
	CurrentSlaveStatus *proto.ReplicationStatus

	// Schema that will be returned by GetSchema. If nil we'll
	// return an error.
	Schema *proto.SchemaDefinition

	// DbaConnectionFactory is the factory for making fake dba connections
	DbaConnectionFactory func() (dbconnpool.PoolConnection, error)

	// DbAppConnectionFactory is the factory for making fake db app connections
	DbAppConnectionFactory func() (dbconnpool.PoolConnection, error)
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

// GetSchema is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) GetSchema(dbName string, tables, excludeTables []string, includeViews bool) (*proto.SchemaDefinition, error) {
	if fmd.Schema == nil {
		return nil, fmt.Errorf("no schema defined")
	}
	return fmd.Schema.FilterTables(tables, excludeTables, includeViews)
}

// GetDbConnection is part of the MysqlDaemon interface
func (fmd *FakeMysqlDaemon) GetDbConnection(dbconfigName dbconfigs.DbConfigName) (dbconnpool.PoolConnection, error) {
	switch dbconfigName {
	case dbconfigs.DbaConfigName:
		if fmd.DbaConnectionFactory == nil {
			return nil, fmt.Errorf("no DbaConnectionFactory set in this FakeMysqlDaemon")
		}
		return fmd.DbaConnectionFactory()
	case dbconfigs.AppConfigName:
		if fmd.DbAppConnectionFactory == nil {
			return nil, fmt.Errorf("no DbAppConnectionFactory set in this FakeMysqlDaemon")
		}
		return fmd.DbAppConnectionFactory()
	}
	return nil, fmt.Errorf("unknown dbconfigName: %v", dbconfigName)
}

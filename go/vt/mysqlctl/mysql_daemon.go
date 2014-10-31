// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"fmt"

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

	// GetDbaConnection returns a connection to be able to talk
	// to the database as the admin user.
	GetDbaConnection() (dbconnpool.PoolConnection, error)
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

	// DbaConnectionFactory is the factory for making fake dba connection
	DbaConnectionFactory func() (dbconnpool.PoolConnection, error)
}

func (fmd *FakeMysqlDaemon) GetMasterAddr() (string, error) {
	if fmd.MasterAddr == "" {
		return "", ErrNotSlave
	}
	if fmd.MasterAddr == "ERROR" {
		return "", fmt.Errorf("FakeMysqlDaemon.GetMasterAddr returns an error")
	}
	return fmd.MasterAddr, nil
}

func (fmd *FakeMysqlDaemon) GetMysqlPort() (int, error) {
	if fmd.MysqlPort == -1 {
		return 0, fmt.Errorf("FakeMysqlDaemon.GetMysqlPort returns an error")
	}
	return fmd.MysqlPort, nil
}

func (fmd *FakeMysqlDaemon) StartSlave(hookExtraEnv map[string]string) error {
	fmd.Replicating = true
	return nil
}

func (fmd *FakeMysqlDaemon) StopSlave(hookExtraEnv map[string]string) error {
	fmd.Replicating = false
	return nil
}

func (fmd *FakeMysqlDaemon) SlaveStatus() (*proto.ReplicationStatus, error) {
	if fmd.CurrentSlaveStatus == nil {
		return nil, fmt.Errorf("no slave status defined")
	}
	return fmd.CurrentSlaveStatus, nil
}

func (fmd *FakeMysqlDaemon) GetSchema(dbName string, tables, excludeTables []string, includeViews bool) (*proto.SchemaDefinition, error) {
	if fmd.Schema == nil {
		return nil, fmt.Errorf("no schema defined")
	}
	return fmd.Schema, nil
}

func (fmd *FakeMysqlDaemon) GetDbaConnection() (dbconnpool.PoolConnection, error) {
	if fmd.DbaConnectionFactory == nil {
		return nil, fmt.Errorf("no DbaConnectionFactory set in this FakeMysqlDaemon")
	}
	return fmd.DbaConnectionFactory()
}

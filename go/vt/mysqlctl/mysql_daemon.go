// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"fmt"
)

// MysqlDaemon is the interface we use for abstracting Mysqld.
type MysqlDaemon interface {
	// GetMasterAddr returns the mysql master address, as shown by
	// 'show slave status'.
	GetMasterAddr() (string, error)

	// GetMysqlPort returns the current port mysql is listening on.
	GetMysqlPort() (int, error)
}

// FakeMysqlDaemon implements MysqlDaemon and allows the user to fake
// everything.
type FakeMysqlDaemon struct {
	// will be returned by GetMasterAddr(). Set to "" to return
	// ErrNotSlave, or to "ERROR" to return an error.
	MasterAddr string

	// will be returned by GetMysqlPort(). Set to -1 to return an error.
	MysqlPort int
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

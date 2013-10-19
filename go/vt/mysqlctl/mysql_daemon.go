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
}

// FakeMysqlDaemon implements MysqlDaemon and allows the user to fake
// everything.
type FakeMysqlDaemon struct {
	// will be returned by GetMasterAddr(). Set to "" to return an error.
	MasterAddr string
}

func (fmd *FakeMysqlDaemon) GetMasterAddr() (string, error) {
	if fmd.MasterAddr == "" {
		return "", fmt.Errorf("FakeMysqlDaemon.GetMasterAddr returns an error")
	}
	return fmd.MasterAddr, nil
}

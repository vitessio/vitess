// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
The TabletServer RPC service handles commands from the wrangler.
*/
package tabletmanager

import (
	"fmt"

	"code.google.com/p/vitess.x/go/vt/mysqlctl"
	"code.google.com/p/vitess.x/go/vt/rpc"
	"code.google.com/p/vitess/go/relog"
)

type TabletManager struct {
	addr   string
	tablet *Tablet
	mysqld *mysqlctl.Mysqld
}

func NewTabletManager(addr string, tablet *Tablet, mysqld *mysqlctl.Mysqld) *TabletManager {
	return &TabletManager{addr, tablet, mysqld}
}

// fatten up an error so it has more information when debugging
func (tm *TabletManager) wrapErr(err error) error {
	if err == nil {
		return err
	}
	relog.Error("%v", err)
	return fmt.Errorf("%v (%v)", err, tm.addr)
}

// Return slave position in terms of the master logs.
func (tm *TabletManager) SlavePosition(_ *rpc.UnusedRequest, reply *mysqlctl.ReplicationPosition) (err error) {
	relog.Debug("SlavePosition")
	position, err := tm.mysqld.SlaveStatus()
	if err == nil {
		*reply = *position
	}
	return tm.wrapErr(err)
}

func (tm *TabletManager) MasterPosition(_ *rpc.UnusedRequest, reply *mysqlctl.ReplicationPosition) (err error) {
	relog.Debug("MasterPosition")
	position, err := tm.mysqld.MasterStatus()
	if err == nil {
		*reply = *position
	}
	relog.Debug("MasterPosition %#v %v", reply, err)
	return tm.wrapErr(err)
}

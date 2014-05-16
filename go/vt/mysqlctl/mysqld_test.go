// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"testing"
)

// FIXME: This test is rather klunky - if something goes wrong it may
// leave mysqld processes behind.

func TestStartShutdown(t *testing.T) {
	mycnf0 := NewMycnf(0, 3700)
	dbaConfig0 := dbconfigs.DefaultDBConfigs.Dba
	replConfig0 := dbconfigs.DefaultDBConfigs.Repl
	tablet0 := NewMysqld(mycnf0, &dbaConfig0, &replConfig0)

	mycnf1 := NewMycnf(1, 3701)
	dbaConfig1 := dbconfigs.DefaultDBConfigs.Dba
	replConfig1 := dbconfigs.DefaultDBConfigs.Repl
	tablet1 := NewMysqld(mycnf1, &dbaConfig1, &replConfig1)
	var err error

	err = Init(tablet0, MysqlWaitTime)
	if err != nil {
		t.Errorf("Init(0) err: %v", err)
	}

	err = Init(tablet1, MysqlWaitTime)

	if err != nil {
		t.Errorf("Init(1) err: %v", err)
	}

	err = Shutdown(tablet0, true, MysqlWaitTime)
	if err != nil {
		t.Errorf("Shutdown() err: %v", err)
	}

	err = Start(tablet0, MysqlWaitTime)
	if err != nil {
		t.Errorf("Start() err: %v", err)
	}

	err = Teardown(tablet0, false)
	if err != nil {
		t.Errorf("Teardown(0) err: %v", err)
	}
	err = Teardown(tablet1, false)
	if err != nil {
		t.Errorf("Teardown(1) err: %v", err)
	}
}

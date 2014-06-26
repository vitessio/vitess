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
	tablet0 := NewMysqld("Dba1", mycnf0, &dbaConfig0, &replConfig0)
	defer tablet0.Close()

	mycnf1 := NewMycnf(1, 3701)
	dbaConfig1 := dbconfigs.DefaultDBConfigs.Dba
	replConfig1 := dbconfigs.DefaultDBConfigs.Repl
	tablet1 := NewMysqld("Dba2", mycnf1, &dbaConfig1, &replConfig1)
	defer tablet1.Close()
	var err error

	err = tablet0.Init(MysqlWaitTime)
	if err != nil {
		t.Errorf("Init(0) err: %v", err)
	}

	err = tablet1.Init(MysqlWaitTime)

	if err != nil {
		t.Errorf("Init(1) err: %v", err)
	}

	err = tablet0.Shutdown(true, MysqlWaitTime)
	if err != nil {
		t.Errorf("Shutdown() err: %v", err)
	}

	err = tablet0.Start(MysqlWaitTime)
	if err != nil {
		t.Errorf("Start() err: %v", err)
	}

	err = tablet0.Teardown(false)
	if err != nil {
		t.Errorf("Teardown(0) err: %v", err)
	}
	err = tablet1.Teardown(false)
	if err != nil {
		t.Errorf("Teardown(1) err: %v", err)
	}
}

// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"testing"
)

// FIXME: This test is rather klunky - if something goes wrong it may
// leave mysqld processes behind.

func TestStartShutdown(t *testing.T) {
	mycnf0 := NewMycnf(0, 3700, VtReplParams{})
	mycnf1 := NewMycnf(1, 3701, VtReplParams{})
	tablet0 := NewMysqld(mycnf0, DefaultDbaParams, DefaultReplParams)
	tablet1 := NewMysqld(mycnf1, DefaultDbaParams, DefaultReplParams)
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

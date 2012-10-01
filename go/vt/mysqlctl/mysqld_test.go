// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"testing"
)

func TestStartShutdown(t *testing.T) {
	mycnf0 := NewMycnf(0, 3700, VtReplParams{})
	mycnf1 := NewMycnf(1, 3701, VtReplParams{})
	tablet0 := NewMysqld(mycnf0, DefaultDbaParams, DefaultReplParams)
	tablet1 := NewMysqld(mycnf1, DefaultDbaParams, DefaultReplParams)
	var err error

	err = Init(tablet0)
	if err != nil {
		t.Fatalf("Init(0) err: %v", err)
	}

	err = Init(tablet1)
	if err != nil {
		t.Fatalf("Init(1) err: %v", err)
	}

	err = Shutdown(tablet0, true)
	if err != nil {
		t.Fatalf("Shutdown() err: %v", err)
	}

	err = Start(tablet0)
	if err != nil {
		t.Fatalf("Start() err: %v", err)
	}

	err = Teardown(tablet0, false)
	if err != nil {
		t.Fatalf("Teardown(0) err: %v", err)
	}
	err = Teardown(tablet1, false)
	if err != nil {
		t.Fatalf("Teardown(1) err: %v", err)
	}
}

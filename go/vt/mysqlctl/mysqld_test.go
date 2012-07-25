// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import "testing"

func TestStartShutdown(t *testing.T) {
	tablet0 := NewMysqld(NewMycnf(0, ""), nil)
	tablet1 := NewMysqld(NewMycnf(1, ""), nil)
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

	err = Teardown(tablet0)
	if err != nil {
		t.Fatalf("Teardown(0) err: %v", err)
	}
	err = Teardown(tablet1)
	if err != nil {
		t.Fatalf("Teardown(1) err: %v", err)
	}
}

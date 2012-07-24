// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"os"
	"testing"
)

func TestMycnf(t *testing.T) {
	var vtRepl VtReplParams
	vtRepl.TabletHost = "localhost"
	vtRepl.TabletPort = 6702
	vtRepl.StartKey = ""
	vtRepl.EndKey = ""

	tablet0 := NewMysqld(NewMycnf(0, 6802, "", vtRepl), nil, nil)
	cnfTemplatePath := os.ExpandEnv("$VTROOT/src/code.google.com/p/vitess/config/mycnf")
	// FIXME(msolomon) make a path that has a chance of succeeding elsewhere
	data, err := MakeMycnfForMysqld(tablet0, cnfTemplatePath, "test header")
	if err != nil {
		t.Errorf("err: %v", err)
	} else {
		t.Logf("data: %v", data)
	}
}

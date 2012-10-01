// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"io/ioutil"
	"os"
	"testing"
)

var MYCNF_PATH = "/tmp/my.cnf"

func TestMycnf(t *testing.T) {
	var vtRepl VtReplParams
	vtRepl.StartKey = ""
	vtRepl.EndKey = ""

	tablet0 := NewMysqld(NewMycnf(0, 6802, vtRepl), DefaultDbaParams, DefaultReplParams)
	cnfTemplatePath := os.ExpandEnv("$VTROOT/src/code.google.com/p/vitess/config/mycnf")
	// FIXME(msolomon) make a path that has a chance of succeeding elsewhere
	data, err := MakeMycnfForMysqld(tablet0, cnfTemplatePath, "test header")
	if err != nil {
		t.Errorf("err: %v", err)
	} else {
		t.Logf("data: %v", data)
	}
	err = ioutil.WriteFile(MYCNF_PATH, []byte(data), 0666)
	if err != nil {
		t.Errorf("failed creating my.cnf %v", err)
	}
	_, err = ioutil.ReadFile(MYCNF_PATH)
	if err != nil {
		t.Errorf("failed reading, err %v", err)
		return
	}
	mycnf, err := ReadMycnf(MYCNF_PATH)
	if err != nil {
		t.Errorf("failed reading, err %v", err)
	} else {
		t.Logf("socket file %v", mycnf.SocketFile)
	}
}

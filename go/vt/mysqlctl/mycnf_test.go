// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"io/ioutil"
	"path"
	"testing"

	"code.google.com/p/vitess/go/vt/env"
)

var MYCNF_PATH = "/tmp/my.cnf"

func TestMycnf(t *testing.T) {
	var vtRepl VtReplParams
	vtRepl.StartKey = ""
	vtRepl.EndKey = ""

	tablet0 := NewMysqld(NewMycnf(0, 6802, vtRepl), DefaultDbaParams, DefaultReplParams)
	root, err := env.VtRoot()
	if err != nil {
		t.Errorf("err: %v", err)
	}
	cnfTemplatePaths := []string{
		path.Join(root, "src/code.google.com/p/vitess/config/mycnf/default.cnf"),
		path.Join(root, "src/code.google.com/p/vitess/config/mycnf/replica.cnf"),
		path.Join(root, "src/code.google.com/p/vitess/config/mycnf/master.cnf"),
	}
	data, err := MakeMycnf(tablet0.config, cnfTemplatePaths)
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

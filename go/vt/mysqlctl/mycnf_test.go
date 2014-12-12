// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/henryanand/vitess/go/vt/dbconfigs"
	"github.com/henryanand/vitess/go/vt/env"
)

var MYCNF_PATH = "/tmp/my.cnf"

func TestMycnf(t *testing.T) {
	os.Setenv("MYSQL_FLAVOR", "GoogleMysql")
	dbaConfig := dbconfigs.DefaultDBConfigs.Dba
	replConfig := dbconfigs.DefaultDBConfigs.Repl
	tablet0 := NewMysqld("Dba", NewMycnf(0, 6802), &dbaConfig, &replConfig)
	defer tablet0.Close()
	root, err := env.VtRoot()
	if err != nil {
		t.Errorf("err: %v", err)
	}
	cnfTemplatePaths := []string{
		path.Join(root, "src/github.com/henryanand/vitess/config/mycnf/default.cnf"),
		path.Join(root, "src/github.com/henryanand/vitess/config/mycnf/replica.cnf"),
		path.Join(root, "src/github.com/henryanand/vitess/config/mycnf/master.cnf"),
	}
	data, err := tablet0.config.makeMycnf(cnfTemplatePaths)
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

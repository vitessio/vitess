// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/vt/env"
)

var MycnfPath = "/tmp/my.cnf"

func TestMycnf(t *testing.T) {
	os.Setenv("MYSQL_FLAVOR", "MariaDB")
	cnf := NewMycnf(11111, 6802)
	// Assigning ServerID to be different from tablet UID to make sure that there are no
	// assumptions in the code that those IDs are the same.
	cnf.ServerID = 22222
	root, err := env.VtRoot()
	if err != nil {
		t.Errorf("err: %v", err)
	}
	cnfTemplatePaths := []string{
		path.Join(root, "src/github.com/youtube/vitess/config/mycnf/default.cnf"),
		path.Join(root, "src/github.com/youtube/vitess/config/mycnf/replica.cnf"),
		path.Join(root, "src/github.com/youtube/vitess/config/mycnf/master.cnf"),
	}
	data, err := cnf.makeMycnf(cnfTemplatePaths)
	if err != nil {
		t.Errorf("err: %v", err)
	} else {
		t.Logf("data: %v", data)
	}
	err = ioutil.WriteFile(MycnfPath, []byte(data), 0666)
	if err != nil {
		t.Errorf("failed creating my.cnf %v", err)
	}
	_, err = ioutil.ReadFile(MycnfPath)
	if err != nil {
		t.Errorf("failed reading, err %v", err)
		return
	}
	mycnf, err := ReadMycnf(MycnfPath)
	if err != nil {
		t.Errorf("failed reading, err %v", err)
	} else {
		t.Logf("socket file %v", mycnf.SocketFile)
	}
	// Tablet UID should be 11111, which determines tablet/data dir.
	if got, want := mycnf.DataDir, "/vt_0000011111/"; !strings.Contains(got, want) {
		t.Errorf("mycnf.DataDir = %v, want *%v*", got, want)
	}
	// MySQL server-id should be 22222, different from Tablet UID.
	if got, want := mycnf.ServerID, uint32(22222); got != want {
		t.Errorf("mycnf.ServerID = %v, want %v", got, want)
	}
}

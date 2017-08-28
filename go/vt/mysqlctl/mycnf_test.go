/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
	uid := uint32(11111)
	cnf := NewMycnf(uid, 6802)
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
	mycnf := NewMycnf(uid, 0)
	mycnf.path = MycnfPath
	mycnf, err = ReadMycnf(mycnf)
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

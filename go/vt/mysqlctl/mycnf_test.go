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

	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/env"
	"vitess.io/vitess/go/vt/servenv"
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
		path.Join(root, "src/vitess.io/vitess/config/mycnf/default.cnf"),
		path.Join(root, "src/vitess.io/vitess/config/mycnf/replica.cnf"),
		path.Join(root, "src/vitess.io/vitess/config/mycnf/master.cnf"),
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

// Run this test if any changes are made to hook handling / make_mycnf hook
// other tests fail if we keep the hook around
// 1. ln -snf $VTTOP/test/vthook-make_mycnf $VTROOT/vthook/make_mycnf
// 2. Remove "No" prefix from func name
// 3. go test
// 4. \rm $VTROOT/vthook/make_mycnf
// 5. Add No Prefix back

func NoTestMycnfHook(t *testing.T) {
	os.Setenv("MYSQL_FLAVOR", "MariaDB")
	uid := uint32(11111)
	cnf := NewMycnf(uid, 6802)
	// Assigning ServerID to be different from tablet UID to make sure that there are no
	// assumptions in the code that those IDs are the same.
	cnf.ServerID = 22222

	// expect these in the output my.cnf
	os.Setenv("KEYSPACE", "test-messagedb")
	os.Setenv("SHARD", "0")
	os.Setenv("TABLET_TYPE", "MASTER")
	os.Setenv("TABLET_ID", "11111")
	os.Setenv("TABLET_DIR", TabletDir(uid))
	// this is not being passed, so it should be nil
	os.Setenv("MY_VAR", "myvalue")

	dbcfgs, err := dbconfigs.Init(cnf.SocketFile)
	mysqld := NewMysqld(dbcfgs)
	servenv.OnClose(mysqld.Close)

	err = mysqld.InitConfig(cnf)
	if err != nil {
		t.Errorf("err: %v", err)
	}
	_, err = ioutil.ReadFile(cnf.path)
	if err != nil {
		t.Errorf("failed reading, err %v", err)
		return
	}
	mycnf := NewMycnf(uid, 0)
	mycnf.path = cnf.path
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
	// check that the env variables we set were passed correctly to the hook
	if got, want := mycnf.lookup("KEYSPACE"), "test-messagedb"; got != want {
		t.Errorf("Error passing env %v, got %v, want %v", "KEYSPACE", got, want)
	}
	if got, want := mycnf.lookup("SHARD"), "0"; got != want {
		t.Errorf("Error passing env %v, got %v, want %v", "SHARD", got, want)
	}
	if got, want := mycnf.lookup("TABLET_TYPE"), "MASTER"; got != want {
		t.Errorf("Error passing env %v, got %v, want %v", "TABLET_TYPE", got, want)
	}
	if got, want := mycnf.lookup("TABLET_ID"), "11111"; got != want {
		t.Errorf("Error passing env %v, got %v, want %v", "TABLET_ID", got, want)
	}
	if got, want := mycnf.lookup("TABLET_DIR"), "/vt_0000011111"; !strings.Contains(got, want) {
		t.Errorf("Error passing env %v, got %v, want %v", "TABLET_DIR", got, want)
	}
	if got := mycnf.lookup("MY_VAR"); got != "" {
		t.Errorf("Unexpected env %v set to %v", "MY_VAR", got)
	}
}

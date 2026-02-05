/*
Copyright 2019 The Vitess Authors.

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
	"bytes"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"

	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/servenv"
)

var MycnfPath = "/tmp/my.cnf"

func TestMycnf(t *testing.T) {
	// Remove any my.cnf file if it already exists.
	os.Remove(MycnfPath)

	uid := uint32(11111)
	cnf := NewMycnf(uid, 6802)
	myTemplateSource := new(bytes.Buffer)
	myTemplateSource.WriteString("[mysqld]\n")
	// Assigning ServerID to be different from tablet UID to make sure that there are no
	// assumptions in the code that those IDs are the same.
	cnf.ServerID = 22222
	f, _ := os.ReadFile("../../../config/mycnf/default.cnf")
	myTemplateSource.Write(f)
	data, err := cnf.makeMycnf(myTemplateSource.String())
	require.NoError(t, err)
	t.Logf("data: %v", data)

	// Since there is no my.cnf file, reading it should fail with a no such file error.
	mycnf := NewMycnf(uid, 0)
	mycnf.Path = MycnfPath
	_, err = ReadMycnf(mycnf, 0)
	require.ErrorContains(t, err, "no such file or directory")

	// Next up we will spawn a go-routine to try and read the cnf file with a timeout.
	// We will create the cnf file after some delay and verify that ReadMycnf does wait that long
	// and ends up succeeding in reading the my.cnf file.
	waitTime := 1 * time.Second
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		startTime := time.Now()
		var readErr error
		mycnf, readErr = ReadMycnf(mycnf, 1*time.Minute)
		require.NoError(t, readErr, "failed reading")
		t.Logf("socket file %v", mycnf.SocketFile)
		totalTimeSpent := time.Since(startTime)
		require.GreaterOrEqual(t, totalTimeSpent, waitTime)
	}()

	time.Sleep(waitTime)
	err = os.WriteFile(MycnfPath, []byte(data), 0666)
	require.NoError(t, err, "failed creating my.cnf")
	_, err = os.ReadFile(MycnfPath)
	require.NoError(t, err, "failed reading")

	// Wait for ReadMycnf to finish and then verify that the data read is correct.
	wg.Wait()
	// Tablet UID should be 11111, which determines tablet/data dir.
	require.Contains(t, mycnf.DataDir, "/vt_0000011111/")
	// MySQL server-id should be 22222, different from Tablet UID.
	require.EqualValues(t, uint32(22222), mycnf.ServerID)
}

// Run this test if any changes are made to hook handling / make_mycnf hook
// other tests fail if we keep the hook around
// 1. ln -snf $VTROOT/test/vthook-make_mycnf $VTROOT/vthook/make_mycnf
// 2. Remove "No" prefix from func name
// 3. go test
// 4. \rm $VTROOT/vthook/make_mycnf
// 5. Add No Prefix back

// nolint
func NoTestMycnfHook(t *testing.T) {
	uid := uint32(11111)
	cnf := NewMycnf(uid, 6802)
	// Assigning ServerID to be different from tablet UID to make sure that there are no
	// assumptions in the code that those IDs are the same.
	cnf.ServerID = 22222

	// expect these in the output my.cnf
	os.Setenv("KEYSPACE", "test-messagedb")
	os.Setenv("SHARD", "0")
	os.Setenv("TABLET_TYPE", "PRIMARY")
	os.Setenv("TABLET_ID", "11111")
	os.Setenv("TABLET_DIR", TabletDir(uid))
	os.Setenv("MYSQL_PORT", "15306")
	// this is not being passed, so it should be nil
	os.Setenv("MY_VAR", "myvalue")

	dbconfigs.GlobalDBConfigs.InitWithSocket(cnf.SocketFile, collations.MySQL8())
	mysqld := NewMysqld(&dbconfigs.GlobalDBConfigs)
	servenv.OnClose(mysqld.Close)

	err := mysqld.InitConfig(cnf)
	require.NoError(t, err)

	_, err = os.ReadFile(cnf.Path)
	require.NoError(t, err)

	mycnf := NewMycnf(uid, 0)
	mycnf.Path = cnf.Path
	mycnf, err = ReadMycnf(mycnf, 0)
	if err != nil {
		t.Errorf("failed reading, err %v", err)
	} else {
		t.Logf("socket file %v", mycnf.SocketFile)
	}
	// Tablet UID should be 11111, which determines tablet/data dir.
	assert.Contains(t, mycnf.DataDir, "/vt_0000011111/")

	// MySQL server-id should be 22222, different from Tablet UID.
	assert.Equal(t, uint32(22222), mycnf.ServerID)

	// check that the env variables we set were passed correctly to the hook
	assert.Equal(t, "test-messagedb", mycnf.lookup("KEYSPACE"))
	assert.Equal(t, "test-0", mycnf.lookup("SHARD"))
	assert.Equal(t, "PRIMARY", mycnf.lookup("TABLET_TYPE"))
	assert.Equal(t, "11111", mycnf.lookup("TABLET_ID"))
	assert.Equal(t, "/vt_0000011111", mycnf.lookup("TABLET_DIR"))
	assert.Equal(t, "15306", mycnf.lookup("MYSQL_PORT"))
	assert.Equal(t, "", mycnf.lookup("MY_VAR"))
}

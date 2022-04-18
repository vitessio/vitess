/*
Copyright 2020 The Vitess Authors.

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

package tabletmanager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/mysqlctl/filebackupstorage"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

func TestBackupBasic(t *testing.T) {
	//defer func(saved bool) { *mysqlctl.DisableActiveReparents = saved }(*mysqlctl.DisableActiveReparents)
	//*mysqlctl.DisableActiveReparents = true
	for i := 0; i < 1; i++ {
		backupRoot := "/Users/rameezsajwani/tmp/rpc_backup_test"
		*filebackupstorage.FileBackupStorageRoot = backupRoot
		// *s3backupstorage.ToInit = "init"
		// *testutil.FakeBackupStorageRoot = "fakepath/"
		*backupstorage.BackupStorageImplementation = "file"
		ctx := context.Background()
		ts := memorytopo.NewServer("cell1")
		tm := newTestTM(t, ts, 1, "ks", "0")
		tm.Cnf = &mysqlctl.Mycnf{
			InnodbDataHomeDir:     "/testdata/Innodb",
			InnodbLogGroupHomeDir: "/testdata/log",
			DataDir:               "/testdata/data",
			// InnodbLogGroupHomeDir: path.Join(backupRoot, "log"),
			// DataDir:               path.Join(backupRoot, "datadir"),
		}
		defer tm.Stop()
		var actual = tm.Tablet().Type
		//fmt.Printf("tablet type is %v\n", tm.Tablet().Type)
		err := tm.Backup(ctx, 4, logutil.NewConsoleLogger(), false)
		assert.True(t, err == nil)
		//assert.Equal(t, "can't find files to backup: open testdata/rpc_backup_test/innodb: no such file or directory", err.Error(), "backup is failing at unexpected place.")
		var after = tm.Tablet().Type
		assert.Equal(t, actual, after)
		//fmt.Printf("tablet type is %v\n", tm.Tablet().Type)
	}
}

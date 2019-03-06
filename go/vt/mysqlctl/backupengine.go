/*
Copyright 2019 The Vitess Authors

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
	"context"
	"flag"
	"fmt"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
)

var (
	// BackupEngineImplementation is the implementation to use
	// for BackupEngine. Exported for test purposes.
	BackupEngineImplementation = flag.String("backup_engine_implementation", "builtin", "which implementation to use for the backup storage engine")
)

// BackupEngine is the interface to the backup engine
type BackupEngine interface {
	ExecuteBackup(ctx context.Context, cnf *Mycnf, mysqld MysqlDaemon, logger logutil.Logger, bh backupstorage.BackupHandle, backupConcurrency int, hookExtraEnv map[string]string) (bool, error)
	ExecuteRestore(ctx context.Context, cnf *Mycnf, mysqld MysqlDaemon, logger logutil.Logger, dir string, bhs []backupstorage.BackupHandle, restoreConcurrency int, hookExtraEnv map[string]string) (mysql.Position, error)
}

// BackupEngineMap contains the registered implementations for BackupEngine
var BackupEngineMap = make(map[string]BackupEngine)

// GetBackupEngine returns the current BackupEngine implementation.
// Should be called after flags have been initialized.
func GetBackupEngine() (BackupEngine, error) {
	be, ok := BackupEngineMap[*BackupEngineImplementation]
	if !ok {
		return nil, fmt.Errorf("no registered implementation of BackupEngine")
	}
	return be, nil
}

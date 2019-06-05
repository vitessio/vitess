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
	"encoding/json"
	"errors"
	"flag"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	// BackupEngineImplementation is the implementation to use for BackupEngine
	backupEngineImplementation = flag.String("backup_engine_implementation", builtin, "which implementation to use for the backup method, builtin or xtrabackup")
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
	be, ok := BackupEngineMap[*backupEngineImplementation]
	if !ok {
		return nil, vterrors.New(vtrpc.Code_NOT_FOUND, "no registered implementation of BackupEngine")
	}
	return be, nil
}

func findBackupToRestore(ctx context.Context, cnf *Mycnf, mysqld MysqlDaemon, logger logutil.Logger, dir string, bhs []backupstorage.BackupHandle, bm interface{}) (backupstorage.BackupHandle, error) {
	var bh backupstorage.BackupHandle
	var index int

	for index = len(bhs) - 1; index >= 0; index-- {
		bh = bhs[index]
		rc, err := bh.ReadFile(ctx, backupManifest)
		if err != nil {
			log.Warningf("Possibly incomplete backup %v in directory %v on BackupStorage: can't read MANIFEST: %v)", bh.Name(), dir, err)
			continue
		}

		err = json.NewDecoder(rc).Decode(&bm)
		rc.Close()
		if err != nil {
			log.Warningf("Possibly incomplete backup %v in directory %v on BackupStorage (cannot JSON decode MANIFEST: %v)", bh.Name(), dir, err)
			continue
		}

		logger.Infof("Restore: found backup %v %v to restore", bh.Directory(), bh.Name())
		break
	}
	if index < 0 {
		// There is at least one attempted backup, but none could be read.
		// This implies there is data we ought to have, so it's not safe to start
		// up empty.
		return nil, errors.New("backup(s) found but none could be read, unsafe to start up empty, restart to retry restore")
	}

	// Starting from here we won't be able to recover if we get stopped by a cancelled
	// context. Thus we use the background context to get through to the finish.

	logger.Infof("Restore: shutdown mysqld")
	err := mysqld.Shutdown(context.Background(), cnf, true)
	if err != nil {
		return nil, err
	}

	logger.Infof("Restore: deleting existing files")
	if err := removeExistingFiles(cnf); err != nil {
		return nil, err
	}

	logger.Infof("Restore: reinit config file")
	err = mysqld.ReinitConfig(context.Background(), cnf)
	if err != nil {
		return nil, err
	}
	return bh, nil
}

/*
Copyright 2022 The Vitess Authors.

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
	"time"

	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
)

type FakeBackupEngine struct {
	ExecuteBackupCalls         []FakeBackupEngineExecuteBackupCall
	ExecuteBackupDuration      time.Duration
	ExecuteBackupReturn        FakeBackupEngineExecuteBackupReturn
	ExecuteRestoreCalls        []FakeBackupEngineExecuteRestoreCall
	ExecuteRestoreDuration     time.Duration
	ExecuteRestoreReturn       FakeBackupEngineExecuteRestoreReturn
	ShouldDrainForBackupCalls  int
	ShouldDrainForBackupReturn bool
}

type FakeBackupEngineExecuteBackupCall struct {
	BackupParams BackupParams
	BackupHandle backupstorage.BackupHandle
}

type FakeBackupEngineExecuteBackupReturn struct {
	Ok  bool
	Err error
}

type FakeBackupEngineExecuteRestoreCall struct {
	BackupHandle  backupstorage.BackupHandle
	RestoreParams RestoreParams
}

type FakeBackupEngineExecuteRestoreReturn struct {
	Manifest *BackupManifest
	Err      error
}

func (be *FakeBackupEngine) ExecuteBackup(
	ctx context.Context,
	params BackupParams,
	bh backupstorage.BackupHandle,
) (bool, error) {
	be.ExecuteBackupCalls = append(be.ExecuteBackupCalls, FakeBackupEngineExecuteBackupCall{params, bh})

	if be.ExecuteBackupDuration > 0 {
		time.Sleep(be.ExecuteBackupDuration)
	}

	return be.ExecuteBackupReturn.Ok, be.ExecuteBackupReturn.Err
}

func (be *FakeBackupEngine) ExecuteRestore(
	ctx context.Context, params RestoreParams,
	bh backupstorage.BackupHandle,
) (*BackupManifest, error) {
	be.ExecuteRestoreCalls = append(be.ExecuteRestoreCalls, FakeBackupEngineExecuteRestoreCall{bh, params})

	// mark restore as in progress
	if err := createStateFile(params.Cnf); err != nil {
		return nil, err
	}

	if be.ExecuteRestoreDuration > 0 {
		time.Sleep(be.ExecuteRestoreDuration)
	}

	return be.ExecuteRestoreReturn.Manifest, be.ExecuteRestoreReturn.Err
}

func (be *FakeBackupEngine) ShouldDrainForBackup() bool {
	be.ShouldDrainForBackupCalls = be.ShouldDrainForBackupCalls + 1
	return be.ShouldDrainForBackupReturn
}

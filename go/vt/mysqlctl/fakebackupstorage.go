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
	"fmt"
	"io"

	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
)

type FakeBackupHandle struct {
	Dir      string
	NameV    string
	ReadOnly bool
	Errors   concurrency.AllErrorRecorder

	AbortBackupCalls  []context.Context
	AbortBackupReturn error
	AddFileCalls      []FakeBackupHandleAddFileCall
	AddFileReturn     FakeBackupHandleAddFileReturn
	EndBackupCalls    []context.Context
	EndBackupReturn   error
	ReadFileCalls     []FakeBackupHandleReadFileCall
	ReadFileReturnF   func(ctx context.Context, filename string) (io.ReadCloser, error)
}

type FakeBackupHandleAddFileCall struct {
	Ctx      context.Context
	Filename string
	Filesize int64
}

type FakeBackupHandleAddFileReturn struct {
	WriteCloser io.WriteCloser
	Err         error
}

type FakeBackupHandleReadFileCall struct {
	Ctx      context.Context
	Filename string
}

func (fbh *FakeBackupHandle) RecordError(err error) {
	fbh.Errors.RecordError(err)
}

func (fbh *FakeBackupHandle) HasErrors() bool {
	return fbh.Errors.HasErrors()
}

func (fbh *FakeBackupHandle) Error() error {
	return fbh.Errors.Error()
}

func (fbh *FakeBackupHandle) Directory() string {
	return fbh.Dir
}

func (fbh *FakeBackupHandle) Name() string {
	return fbh.NameV
}

func (fbh *FakeBackupHandle) AddFile(ctx context.Context, filename string, filesize int64) (io.WriteCloser, error) {
	fbh.AddFileCalls = append(fbh.AddFileCalls, FakeBackupHandleAddFileCall{ctx, filename, filesize})
	return fbh.AddFileReturn.WriteCloser, fbh.AddFileReturn.Err
}

func (fbh *FakeBackupHandle) EndBackup(ctx context.Context) error {
	fbh.EndBackupCalls = append(fbh.EndBackupCalls, ctx)
	return fbh.EndBackupReturn
}

func (fbh *FakeBackupHandle) AbortBackup(ctx context.Context) error {
	fbh.AbortBackupCalls = append(fbh.AbortBackupCalls, ctx)
	return fbh.AbortBackupReturn
}

func (fbh *FakeBackupHandle) ReadFile(ctx context.Context, filename string) (io.ReadCloser, error) {
	fbh.ReadFileCalls = append(fbh.ReadFileCalls, FakeBackupHandleReadFileCall{ctx, filename})
	if fbh.ReadFileReturnF == nil {
		return nil, fmt.Errorf("FakeBackupHandle has not defined a ReadFileReturnF")
	}
	return fbh.ReadFileReturnF(ctx, filename)
}

type FakeBackupStorage struct {
	CloseCalls          int
	CloseReturn         error
	ListBackupsCalls    []FakeBackupStorageListBackupsCall
	ListBackupsReturn   FakeBackupStorageListBackupsReturn
	RemoveBackupCalls   []FakeBackupStorageRemoveBackupCall
	RemoveBackupReturn  error
	RemoveBackupReturne error
	StartBackupCalls    []FakeBackupStorageStartBackupCall
	StartBackupReturn   FakeBackupStorageStartBackupReturn
	WithParamsCalls     []backupstorage.Params
	WithParamsReturn    backupstorage.BackupStorage
}

type FakeBackupStorageListBackupsCall struct {
	Ctx context.Context
	Dir string
}

type FakeBackupStorageListBackupsReturn struct {
	BackupHandles []backupstorage.BackupHandle
	Err           error
}

type FakeBackupStorageRemoveBackupCall struct {
	Ctx  context.Context
	Dir  string
	Name string
}

type FakeBackupStorageStartBackupCall struct {
	Ctx  context.Context
	Dir  string
	Name string
}

type FakeBackupStorageStartBackupReturn struct {
	BackupHandle backupstorage.BackupHandle
	Err          error
}

func (fbs *FakeBackupStorage) ListBackups(ctx context.Context, dir string) ([]backupstorage.BackupHandle, error) {
	fbs.ListBackupsCalls = append(fbs.ListBackupsCalls, FakeBackupStorageListBackupsCall{ctx, dir})
	return fbs.ListBackupsReturn.BackupHandles, fbs.ListBackupsReturn.Err
}

func (fbs *FakeBackupStorage) StartBackup(ctx context.Context, dir, name string) (backupstorage.BackupHandle, error) {
	fbs.StartBackupCalls = append(fbs.StartBackupCalls, FakeBackupStorageStartBackupCall{ctx, dir, name})
	return fbs.StartBackupReturn.BackupHandle, fbs.StartBackupReturn.Err
}

func (fbs *FakeBackupStorage) RemoveBackup(ctx context.Context, dir, name string) error {
	fbs.RemoveBackupCalls = append(fbs.RemoveBackupCalls, FakeBackupStorageRemoveBackupCall{ctx, dir, name})
	return fbs.RemoveBackupReturn
}

func (fbs *FakeBackupStorage) Close() error {
	fbs.CloseCalls = fbs.CloseCalls + 1
	return fbs.CloseReturn
}

func (fbs *FakeBackupStorage) WithParams(params backupstorage.Params) backupstorage.BackupStorage {
	fbs.WithParamsCalls = append(fbs.WithParamsCalls, params)
	return fbs.WithParamsReturn
}

/*
Copyright 2021 The Vitess Authors.

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

package testutil

import (
	"context"
	"sort"

	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
)

type backupStorage struct {
	backupstorage.BackupStorage

	// Backups is a mapping of directory to list of backup names stored in that
	// directory.
	Backups map[string][]string
	// ListBackupsError is returned from ListBackups when it is non-nil.
	ListBackupsError error
}

// ListBackups is part of the backupstorage.BackupStorage interface.
func (bs *backupStorage) ListBackups(ctx context.Context, dir string) ([]backupstorage.BackupHandle, error) {
	if bs.ListBackupsError != nil {
		return nil, bs.ListBackupsError
	}

	handles := []backupstorage.BackupHandle{}

	for k, v := range bs.Backups {
		if k == dir {
			for _, name := range v {
				handles = append(handles, &backupHandle{directory: k, name: name})
			}
		}
	}

	sort.Sort(handlesByName(handles))

	return handles, nil
}

// Close is part of the backupstorage.BackupStorage interface.
func (bs *backupStorage) Close() error { return nil }

// backupHandle implements a subset of the backupstorage.backupHandle interface.
type backupHandle struct {
	backupstorage.BackupHandle

	directory string
	name      string
}

func (bh *backupHandle) Directory() string { return bh.directory }
func (bh *backupHandle) Name() string      { return bh.name }

// handlesByName implements the sort interface for backup handles by Name().
type handlesByName []backupstorage.BackupHandle

func (a handlesByName) Len() int           { return len(a) }
func (a handlesByName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a handlesByName) Less(i, j int) bool { return a[i].Name() < a[j].Name() }

// BackupStorageImplementation is the name this package registers its test
// backupstorage.BackupStorage implementation as. Users should set
// *backupstorage.BackupStorageImplementation to this value before use.
const BackupStorageImplementation = "grpcvtctldserver.testutil"

// BackupStorage is the singleton test backupstorage.BackupStorage intastnce. It
// is public and singleton to allow tests to both mutate and assert against its
// state.
var BackupStorage = &backupStorage{
	Backups: map[string][]string{},
}

func init() {
	backupstorage.BackupStorageMap[BackupStorageImplementation] = BackupStorage
}

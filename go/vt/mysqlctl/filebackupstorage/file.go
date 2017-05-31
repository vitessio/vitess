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

// Package filebackupstorage implements the BackupStorage interface
// for a local filesystem (which can be an NFS mount).
package filebackupstorage

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/mysqlctl/backupstorage"
)

var (
	// FileBackupStorageRoot is where the backups will go.
	// Exported for test purposes.
	FileBackupStorageRoot = flag.String("file_backup_storage_root", "", "root directory for the file backup storage")
)

// FileBackupHandle implements BackupHandle for local file system.
type FileBackupHandle struct {
	fbs      *FileBackupStorage
	dir      string
	name     string
	readOnly bool
}

// Directory is part of the BackupHandle interface
func (fbh *FileBackupHandle) Directory() string {
	return fbh.dir
}

// Name is part of the BackupHandle interface
func (fbh *FileBackupHandle) Name() string {
	return fbh.name
}

// AddFile is part of the BackupHandle interface
func (fbh *FileBackupHandle) AddFile(ctx context.Context, filename string) (io.WriteCloser, error) {
	if fbh.readOnly {
		return nil, fmt.Errorf("AddFile cannot be called on read-only backup")
	}
	p := path.Join(*FileBackupStorageRoot, fbh.dir, fbh.name, filename)
	return os.Create(p)
}

// EndBackup is part of the BackupHandle interface
func (fbh *FileBackupHandle) EndBackup(ctx context.Context) error {
	if fbh.readOnly {
		return fmt.Errorf("EndBackup cannot be called on read-only backup")
	}
	return nil
}

// AbortBackup is part of the BackupHandle interface
func (fbh *FileBackupHandle) AbortBackup(ctx context.Context) error {
	if fbh.readOnly {
		return fmt.Errorf("AbortBackup cannot be called on read-only backup")
	}
	return fbh.fbs.RemoveBackup(ctx, fbh.dir, fbh.name)
}

// ReadFile is part of the BackupHandle interface
func (fbh *FileBackupHandle) ReadFile(ctx context.Context, filename string) (io.ReadCloser, error) {
	if !fbh.readOnly {
		return nil, fmt.Errorf("ReadFile cannot be called on read-write backup")
	}
	p := path.Join(*FileBackupStorageRoot, fbh.dir, fbh.name, filename)
	return os.Open(p)
}

// FileBackupStorage implements BackupStorage for local file system.
type FileBackupStorage struct{}

// ListBackups is part of the BackupStorage interface
func (fbs *FileBackupStorage) ListBackups(ctx context.Context, dir string) ([]backupstorage.BackupHandle, error) {
	// ReadDir already sorts the results
	p := path.Join(*FileBackupStorageRoot, dir)
	fi, err := ioutil.ReadDir(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	result := make([]backupstorage.BackupHandle, 0, len(fi))
	for _, info := range fi {
		if !info.IsDir() {
			continue
		}
		if info.Name() == "." || info.Name() == ".." {
			continue
		}
		result = append(result, &FileBackupHandle{
			fbs:      fbs,
			dir:      dir,
			name:     info.Name(),
			readOnly: true,
		})
	}
	return result, nil
}

// StartBackup is part of the BackupStorage interface
func (fbs *FileBackupStorage) StartBackup(ctx context.Context, dir, name string) (backupstorage.BackupHandle, error) {
	// Make sure the directory exists.
	p := path.Join(*FileBackupStorageRoot, dir)
	if err := os.MkdirAll(p, os.ModePerm); err != nil {
		return nil, err
	}

	// Create the subdirectory for this named backup.
	p = path.Join(p, name)
	if err := os.Mkdir(p, os.ModePerm); err != nil {
		return nil, err
	}

	return &FileBackupHandle{
		fbs:      fbs,
		dir:      dir,
		name:     name,
		readOnly: false,
	}, nil
}

// RemoveBackup is part of the BackupStorage interface
func (fbs *FileBackupStorage) RemoveBackup(ctx context.Context, dir, name string) error {
	p := path.Join(*FileBackupStorageRoot, dir, name)
	return os.RemoveAll(p)
}

// Close implements BackupStorage.
func (fbs *FileBackupStorage) Close() error {
	return nil
}

func init() {
	backupstorage.BackupStorageMap["file"] = &FileBackupStorage{}
}

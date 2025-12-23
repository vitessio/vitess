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

// Package filebackupstorage implements the BackupStorage interface
// for a local filesystem (which can be an NFS mount).
package filebackupstorage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/fileutil"
	"vitess.io/vitess/go/ioutil"
	"vitess.io/vitess/go/os2"
	stats "vitess.io/vitess/go/vt/mysqlctl/backupstats"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	mysqlctlerrors "vitess.io/vitess/go/vt/mysqlctl/errors"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/utils"
)

var (
	// FileBackupStorageRoot is where the backups will go.
	// Exported for test purposes.
	FileBackupStorageRoot string

	defaultFileBackupStorage = newFileBackupStorage(backupstorage.NoParams())
)

func registerFlags(fs *pflag.FlagSet) {
	utils.SetFlagStringVar(fs, &FileBackupStorageRoot, "file-backup-storage-root", "", "Root directory for the file backup storage.")
}

func init() {
	servenv.OnParseFor("vtbackup", registerFlags)
	servenv.OnParseFor("vtctl", registerFlags)
	servenv.OnParseFor("vtctld", registerFlags)
	servenv.OnParseFor("vttablet", registerFlags)
}

// FileBackupHandle implements BackupHandle for local file system.
type FileBackupHandle struct {
	fbs      *FileBackupStorage
	dir      string
	name     string
	readOnly bool
	mysqlctlerrors.PerFileErrorRecorder
}

func NewBackupHandle(
	fbs *FileBackupStorage,
	dir,
	name string,
	readOnly bool,
) backupstorage.BackupHandle {
	if fbs == nil {
		fbs = defaultFileBackupStorage
	}
	return &FileBackupHandle{
		fbs:      fbs,
		dir:      dir,
		name:     name,
		readOnly: readOnly,
	}
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
func (fbh *FileBackupHandle) AddFile(ctx context.Context, filename string, filesize int64) (io.WriteCloser, error) {
	if fbh.readOnly {
		return nil, errors.New("AddFile cannot be called on read-only backup")
	}
	p := path.Join(FileBackupStorageRoot, fbh.dir, fbh.name, filename)
	f, err := os2.Create(p)
	if err != nil {
		return nil, err
	}
	stat := fbh.fbs.params.Stats.Scope(stats.Operation("File:Write"))
	return ioutil.NewMeteredWriteCloser(f, stat.TimedIncrementBytes), nil
}

// EndBackup is part of the BackupHandle interface
func (fbh *FileBackupHandle) EndBackup(ctx context.Context) error {
	if fbh.readOnly {
		return errors.New("EndBackup cannot be called on read-only backup")
	}
	return nil
}

// AbortBackup is part of the BackupHandle interface
func (fbh *FileBackupHandle) AbortBackup(ctx context.Context) error {
	if fbh.readOnly {
		return errors.New("AbortBackup cannot be called on read-only backup")
	}
	return fbh.fbs.RemoveBackup(ctx, fbh.dir, fbh.name)
}

// ReadFile is part of the BackupHandle interface
func (fbh *FileBackupHandle) ReadFile(ctx context.Context, filename string) (io.ReadCloser, error) {
	if !fbh.readOnly {
		return nil, errors.New("ReadFile cannot be called on read-write backup")
	}
	p, err := fileutil.SafePathJoin(FileBackupStorageRoot, fbh.dir, fbh.name, filename)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	stat := fbh.fbs.params.Stats.Scope(stats.Operation("File:Read"))
	return ioutil.NewMeteredReadCloser(f, stat.TimedIncrementBytes), nil
}

// FileBackupStorage implements BackupStorage for local file system.
type FileBackupStorage struct {
	params backupstorage.Params
}

func newFileBackupStorage(params backupstorage.Params) *FileBackupStorage {
	return &FileBackupStorage{params}
}

// ListBackups is part of the BackupStorage interface
func (fbs *FileBackupStorage) ListBackups(ctx context.Context, dir string) ([]backupstorage.BackupHandle, error) {
	// Check dir is not a directory traversal.
	path, err := fileutil.SafePathJoin(FileBackupStorageRoot, dir)
	if err != nil {
		return nil, fmt.Errorf("failed to parse backup path %q: %w", path, err)
	}

	fi, err := os.ReadDir(path)
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
		result = append(result, NewBackupHandle(fbs, dir, info.Name(), true /*readOnly*/))
	}
	return result, nil
}

// StartBackup is part of the BackupStorage interface
func (fbs *FileBackupStorage) StartBackup(ctx context.Context, dir, name string) (backupstorage.BackupHandle, error) {
	// Make sure the directory exists.
	p, err := fileutil.SafePathJoin(FileBackupStorageRoot, dir)
	if err != nil {
		return nil, err
	}
	if err = os2.MkdirAll(p); err != nil {
		return nil, err
	}

	// Create the subdirectory for this named backup.
	p = path.Join(p, name)
	if err = os2.Mkdir(p); err != nil {
		return nil, err
	}

	return NewBackupHandle(fbs, dir, name, false /*readOnly*/), nil
}

// RemoveBackup is part of the BackupStorage interface
func (fbs *FileBackupStorage) RemoveBackup(ctx context.Context, dir, name string) error {
	p, err := fileutil.SafePathJoin(FileBackupStorageRoot, dir, name)
	if err != nil {
		return err
	}
	return os.RemoveAll(p)
}

// Close implements BackupStorage.
func (fbs *FileBackupStorage) Close() error {
	return nil
}

func (fbs *FileBackupStorage) WithParams(params backupstorage.Params) backupstorage.BackupStorage {
	return &FileBackupStorage{params}
}

func init() {
	backupstorage.BackupStorageMap["file"] = defaultFileBackupStorage
}

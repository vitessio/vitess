// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package backupstorage

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
)

// This file contains the flocal file system implementation of the
// BackupStorage interface.

var (
	fileBackupStorageRoot = flag.String("file_backup_storage_root", "", "root directory for the file backup storage")
)

// FileBackupHandle implements BackupHandle for local file system.
type FileBackupHandle struct {
	fbs      *FileBackupStorage
	bucket   string
	name     string
	readOnly bool
}

// Bucket is part of the BackupHandle interface
func (fbh *FileBackupHandle) Bucket() string {
	return fbh.bucket
}

// Name is part of the BackupHandle interface
func (fbh *FileBackupHandle) Name() string {
	return fbh.name
}

// AddFile is part of the BackupHandle interface
func (fbh *FileBackupHandle) AddFile(filename string) (io.WriteCloser, error) {
	if fbh.readOnly {
		return nil, fmt.Errorf("AddFile cannot be called on read-only backup")
	}
	p := path.Join(fbh.fbs.root, fbh.bucket, fbh.name, filename)
	return os.Create(p)
}

// EndBackup is part of the BackupHandle interface
func (fbh *FileBackupHandle) EndBackup() error {
	if fbh.readOnly {
		return fmt.Errorf("EndBackup cannot be called on read-only backup")
	}
	return nil
}

// AbortBackup is part of the BackupHandle interface
func (fbh *FileBackupHandle) AbortBackup() error {
	if fbh.readOnly {
		return fmt.Errorf("AbortBackup cannot be called on read-only backup")
	}
	return fbh.fbs.RemoveBackup(fbh.bucket, fbh.name)
}

// ReadFile is part of the BackupHandle interface
func (fbh *FileBackupHandle) ReadFile(filename string) (io.ReadCloser, error) {
	if !fbh.readOnly {
		return nil, fmt.Errorf("ReadFile cannot be called on read-write backup")
	}
	p := path.Join(fbh.fbs.root, fbh.bucket, fbh.name, filename)
	return os.Open(p)
}

// FileBackupStorage implements BackupStorage for local file system.
type FileBackupStorage struct {
	root string
}

// ListBackups is part of the BackupStorage interface
func (fbs *FileBackupStorage) ListBackups(bucket string) ([]BackupHandle, error) {
	// ReadDir already sorts the results
	p := path.Join(fbs.root, bucket)
	fi, err := ioutil.ReadDir(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	result := make([]BackupHandle, 0, len(fi))
	for _, info := range fi {
		if !info.IsDir() {
			continue
		}
		if info.Name() == "." || info.Name() == ".." {
			continue
		}
		result = append(result, &FileBackupHandle{
			fbs:      fbs,
			bucket:   bucket,
			name:     info.Name(),
			readOnly: true,
		})
	}
	return result, nil
}

// StartBackup is part of the BackupStorage interface
func (fbs *FileBackupStorage) StartBackup(bucket, name string) (BackupHandle, error) {
	// make sure the bucket directory exists
	p := path.Join(fbs.root, bucket)
	if err := os.MkdirAll(p, os.ModePerm); err != nil {
		return nil, err
	}

	// creates the backup directory
	p = path.Join(p, name)
	if err := os.Mkdir(p, os.ModePerm); err != nil {
		return nil, err
	}

	return &FileBackupHandle{
		fbs:      fbs,
		bucket:   bucket,
		name:     name,
		readOnly: false,
	}, nil
}

// RemoveBackup is part of the BackupStorage interface
func (fbs *FileBackupStorage) RemoveBackup(bucket, name string) error {
	p := path.Join(fbs.root, bucket, name)
	return os.RemoveAll(p)
}

// RegisterFileBackupStorage should be called after Flags has been
// initialized, to register the FileBackupStorage implementation
func RegisterFileBackupStorage() {
	if *fileBackupStorageRoot != "" {
		BackupStorageMap["file"] = &FileBackupStorage{
			root: *fileBackupStorageRoot,
		}
	}
}

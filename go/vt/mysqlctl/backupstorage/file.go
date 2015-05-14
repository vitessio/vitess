// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package backupstorage

import (
	"flag"
	"fmt"
	"io"
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
	bucket string
	name   string
}

// Bucket is part of the BackupHandle interface
func (fbh *FileBackupHandle) Bucket() string {
	return fbh.bucket
}

// Name is part of the BackupHandle interface
func (fbh *FileBackupHandle) Name() string {
	return fbh.name
}

// FileBackupStorage implements BackupStorage for local file system.
type FileBackupStorage struct {
	root string
}

// ListBackups is part of the BackupStorage interface
func (fbs *FileBackupStorage) ListBackups(bucket string) ([]BackupHandle, error) {
	p := path.Join(fbs.root, bucket)
	f, err := os.Open(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	fi, err := f.Readdir(-1)
	if err != nil {
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
			bucket: bucket,
			name:   info.Name(),
		})
	}
	SortBackupHandleArray(result)
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
		bucket: bucket,
		name:   name,
	}, nil
}

// AddFile is part of the BackupStorage interface
func (fbs *FileBackupStorage) AddFile(handle BackupHandle, filename string) (io.WriteCloser, error) {
	fbh, ok := handle.(*FileBackupHandle)
	if !ok {
		return nil, fmt.Errorf("FileBackupStorage only accepts FileBackupHandle")
	}
	p := path.Join(fbs.root, fbh.bucket, fbh.name, filename)
	return os.Create(p)
}

// EndBackup is part of the BackupStorage interface
func (fbs *FileBackupStorage) EndBackup(handle BackupHandle) error {
	return nil
}

// AbortBackup is part of the BackupStorage interface
func (fbs *FileBackupStorage) AbortBackup(handle BackupHandle) error {
	fbh, ok := handle.(*FileBackupHandle)
	if !ok {
		return fmt.Errorf("FileBackupStorage only accepts FileBackupHandle")
	}
	return fbs.RemoveBackup(fbh.bucket, fbh.name)
}

// ReadFile is part of the BackupStorage interface
func (fbs *FileBackupStorage) ReadFile(handle BackupHandle, filename string) (io.ReadCloser, error) {
	fbh, ok := handle.(*FileBackupHandle)
	if !ok {
		return nil, fmt.Errorf("FileBackupStorage only accepts FileBackupHandle")
	}
	p := path.Join(fbs.root, fbh.bucket, fbh.name, filename)
	return os.Open(p)
}

// RemoveBackup is part of the BackupStorage interface
func (fbs *FileBackupStorage) RemoveBackup(bucket, name string) error {
	p := path.Join(fbs.root, bucket, name)
	return os.RemoveAll(p)
}

// RegisterFileBackupStorage should be called after Flags has been
// initialized, to register the FileBackupStorage implementation
func RegisterFileBackupStorage() {
	BackupStorageMap["file"] = &FileBackupStorage{
		root: *fileBackupStorageRoot,
	}
}

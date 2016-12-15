// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package backupstorage contains the interface and file system implementation
// of the backup system.
package backupstorage

import (
	"flag"
	"fmt"
	"io"

	"golang.org/x/net/context"
)

var (
	// BackupStorageImplementation is the implementation to use
	// for BackupStorage. Exported for test purposes.
	BackupStorageImplementation = flag.String("backup_storage_implementation", "", "which implementation to use for the backup storage feature")
)

// BackupHandle describes an individual backup.
type BackupHandle interface {
	// Directory is the location of the backup. Will contain keyspace/shard.
	Directory() string

	// Name is the individual name of the backup. Will contain
	// tabletAlias-timestamp.
	Name() string

	// AddFile opens a new file to be added to the backup.
	// Only works for read-write backups (created by StartBackup).
	// filename is guaranteed to only contain alphanumerical
	// characters and hyphens.
	// It should be thread safe, it is possible to call AddFile in
	// multiple go routines once a backup has been started.
	// The context is valid for the duration of the writes, until the
	// WriteCloser is closed.
	AddFile(ctx context.Context, filename string) (io.WriteCloser, error)

	// EndBackup stops and closes a backup. The contents should be kept.
	// Only works for read-write backups (created by StartBackup).
	EndBackup(ctx context.Context) error

	// AbortBackup stops a backup, and removes the contents that
	// have been copied already. It is called if an error occurs
	// while the backup is being taken, and the backup cannot be finished.
	// Only works for read-write backups (created by StartBackup).
	AbortBackup(ctx context.Context) error

	// ReadFile starts reading a file from a backup.
	// Only works for read-only backups (created by ListBackups).
	// The context is valid for the duration of the reads, until the
	// ReadCloser is closed.
	ReadFile(ctx context.Context, filename string) (io.ReadCloser, error)
}

// BackupStorage is the interface to the storage system
type BackupStorage interface {
	// ListBackups returns all the backups in a directory.  The
	// returned backups are read-only (ReadFile can be called, but
	// AddFile/EndBackup/AbortBackup cannot).
	// The backups are string-sorted by Name(), ascending (ends up
	// being the oldest backup first).
	ListBackups(ctx context.Context, dir string) ([]BackupHandle, error)

	// StartBackup creates a new backup with the given name.  If a
	// backup with the same name already exists, it's an error.
	// The returned backup is read-write
	// (AddFile/EndBackup/AbortBackup can all be called, not
	// ReadFile). The provided context is only valid for that
	// function, and should not be stored by the implementation.
	StartBackup(ctx context.Context, dir, name string) (BackupHandle, error)

	// RemoveBackup removes all the data associated with a backup.
	// It will not appear in ListBackups after RemoveBackup succeeds.
	RemoveBackup(ctx context.Context, dir, name string) error

	// Close frees resources associated with an active backup
	// session, such as closing connections. Implementations of
	// BackupStorage must support being reused after Close() is called.
	Close() error
}

// BackupStorageMap contains the registered implementations for BackupStorage
var BackupStorageMap = make(map[string]BackupStorage)

// GetBackupStorage returns the current BackupStorage implementation.
// Should be called after flags have been initialized.
// When all operations are done, call BackupStorage.Close() to free resources.
func GetBackupStorage() (BackupStorage, error) {
	bs, ok := BackupStorageMap[*BackupStorageImplementation]
	if !ok {
		return nil, fmt.Errorf("no registered implementation of BackupStorage")
	}
	return bs, nil
}

// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package backupstorage contains the interface and file system implementation
// of the backup system.
package backupstorage

import (
	"flag"
	"io"
	"sort"

	log "github.com/golang/glog"
)

var (
	backupStorageImplementation = flag.String("backup_storage_implementation", "", "which implementation to use for the backup storage feature")
)

// BackupHandle describes an individual backup.
type BackupHandle interface {
	// Bucket is the location of the backup. Will contain keyspace/shard.
	Bucket() string

	// Name is the individual name of the backup. Will contain
	// tabletAlias-timestamp.
	Name() string
}

// BackupStorage is the interface to the storage system
type BackupStorage interface {
	// ListBackups returns all the backups in a bucket.
	ListBackups(bucket string) ([]BackupHandle, error)

	// StartBackup creates a new backup with the given name.
	// If a backup with the same name already exists, it's an error.
	StartBackup(bucket, name string) (BackupHandle, error)

	// AddFile opens a new file to be added to the backup.
	// filename is guaranteed to only contain alphanumerical
	// characters and hyphens.
	// It should be thread safe, it is possible to call AddFile in
	// multiple go routines once a backup has been started.
	AddFile(handle BackupHandle, filename string) (io.WriteCloser, error)

	// EndBackup stops and closes a backup. The contents should be kept.
	EndBackup(handle BackupHandle) error

	// AbortBackup stops a backup, and removes the contents that
	// have been copied already. It is called if an error occurs
	// while the backup is being taken, and the backup cannot be finished.
	AbortBackup(handle BackupHandle) error

	// ReadFile starts reading a file from a backup.
	ReadFile(handle BackupHandle, filename string) (io.ReadCloser, error)

	// RemoveBackup removes all the data associated with a backup.
	// It will not appear in ListBackups after RemoveBackup succeeds.
	RemoveBackup(bucket, name string) error
}

// Helper code to sort BackupHandle arrays

type byName []BackupHandle

func (bha byName) Len() int           { return len(bha) }
func (bha byName) Swap(i, j int)      { bha[i], bha[j] = bha[j], bha[i] }
func (bha byName) Less(i, j int) bool { return bha[i].Name() < bha[j].Name() }

// SortBackupHandleArray will sort the BackupHandle array by name.
// To be used by implementations on the result of ListBackups.
func SortBackupHandleArray(bha []BackupHandle) {
	sort.Sort(byName(bha))
}

// BackupStorageMap contains the registered implementations for BackupStorage
var BackupStorageMap = make(map[string]BackupStorage)

// GetBackupStorage returns the current BackupStorage implementation.
// Should be called after flags have been initialized.
func GetBackupStorage() BackupStorage {
	bs, ok := BackupStorageMap[*backupStorageImplementation]
	if !ok {
		log.Fatalf("no registered implementation of BackupStorage")
	}
	return bs
}

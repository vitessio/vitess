// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package gcsbackupstorage implements the BackupStorage interface
// for Google Cloud Storage.
package gcsbackupstorage

import (
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"

	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"google.golang.org/cloud"
	"google.golang.org/cloud/storage"

	"github.com/youtube/vitess/go/vt/mysqlctl/backupstorage"
)

var (
	// project is the Google Developers Console project ID.
	project = flag.String("gcs_backup_storage_project", "", "Google Developers Console project ID to use for backups")

	// bucket is where the backups will go.
	bucket = flag.String("gcs_backup_storage_bucket", "", "Google Cloud Storage bucket to use for backups")

	// root is a prefix added to all object names.
	root = flag.String("gcs_backup_storage_root", "", "root prefix for all backup-related object names")
)

// GCSBackupHandle implements BackupHandle for Google Cloud Storage.
type GCSBackupHandle struct {
	authCtx  context.Context
	bs       *GCSBackupStorage
	dir      string
	name     string
	readOnly bool
}

// Directory implements BackupHandle.
func (bh *GCSBackupHandle) Directory() string {
	return bh.dir
}

// Name implements BackupHandle.
func (bh *GCSBackupHandle) Name() string {
	return bh.name
}

// AddFile implements BackupHandle.
func (bh *GCSBackupHandle) AddFile(filename string) (io.WriteCloser, error) {
	if bh.readOnly {
		return nil, fmt.Errorf("AddFile cannot be called on read-only backup")
	}
	return storage.NewWriter(bh.authCtx, *bucket, objName(bh.dir, bh.name, filename)), nil
}

// EndBackup implements BackupHandle.
func (bh *GCSBackupHandle) EndBackup() error {
	if bh.readOnly {
		return fmt.Errorf("EndBackup cannot be called on read-only backup")
	}
	return nil
}

// AbortBackup implements BackupHandle.
func (bh *GCSBackupHandle) AbortBackup() error {
	if bh.readOnly {
		return fmt.Errorf("AbortBackup cannot be called on read-only backup")
	}
	return bh.bs.RemoveBackup(bh.dir, bh.name)
}

// ReadFile implements BackupHandle.
func (bh *GCSBackupHandle) ReadFile(filename string) (io.ReadCloser, error) {
	if !bh.readOnly {
		return nil, fmt.Errorf("ReadFile cannot be called on read-write backup")
	}
	return storage.NewReader(bh.authCtx, *bucket, objName(bh.dir, bh.name, filename))
}

// GCSBackupStorage implements BackupStorage for Google Cloud Storage.
type GCSBackupStorage struct {
	authCtx context.Context
}

// ListBackups implements BackupStorage.
func (bs *GCSBackupStorage) ListBackups(dir string) ([]backupstorage.BackupHandle, error) {
	authCtx, err := bs.authContext()
	if err != nil {
		return nil, err
	}

	// List prefixes that begin with dir (i.e. list subdirs).
	var subdirs []string
	searchPrefix := objName(dir, "" /* include trailing slash */)
	query := &storage.Query{
		Delimiter: "/",
		Prefix:    searchPrefix,
	}

	// Loop in case results are returned in multiple batches.
	for query != nil {
		objs, err := storage.ListObjects(authCtx, *bucket, query)
		if err != nil {
			return nil, err
		}

		// Each returned prefix is a subdir.
		// Strip parent dir from full path.
		for _, prefix := range objs.Prefixes {
			subdir := strings.TrimPrefix(prefix, searchPrefix)
			subdir = strings.TrimSuffix(subdir, "/")
			subdirs = append(subdirs, subdir)
		}

		query = objs.Next
	}

	// Backups must be returned in order, oldest first.
	sort.Strings(subdirs)

	result := make([]backupstorage.BackupHandle, 0, len(subdirs))
	for _, subdir := range subdirs {
		result = append(result, &GCSBackupHandle{
			authCtx:  authCtx,
			bs:       bs,
			dir:      dir,
			name:     subdir,
			readOnly: true,
		})
	}
	return result, nil
}

// StartBackup implements BackupStorage.
func (bs *GCSBackupStorage) StartBackup(dir, name string) (backupstorage.BackupHandle, error) {
	authCtx, err := bs.authContext()
	if err != nil {
		return nil, err
	}

	return &GCSBackupHandle{
		authCtx:  authCtx,
		bs:       bs,
		dir:      dir,
		name:     name,
		readOnly: false,
	}, nil
}

// RemoveBackup implements BackupStorage.
func (bs *GCSBackupStorage) RemoveBackup(dir, name string) error {
	authCtx, err := bs.authContext()
	if err != nil {
		return err
	}

	// Find all objects with the right prefix.
	query := &storage.Query{
		Prefix: objName(dir, name, "" /* include trailing slash */),
	}

	// Loop in case results are returned in multiple batches.
	for query != nil {
		objs, err := storage.ListObjects(authCtx, *bucket, query)
		if err != nil {
			return err
		}

		// Delete all the found objects.
		for _, obj := range objs.Results {
			if err := storage.DeleteObject(authCtx, *bucket, obj.Name); err != nil {
				return fmt.Errorf("unable to delete %q from bucket %q: %v", obj.Name, *bucket, err)
			}
		}

		query = objs.Next
	}

	return nil
}

func (bs *GCSBackupStorage) authContext() (context.Context, error) {
	if bs.authCtx == nil {
		client, err := google.DefaultClient(context.TODO())
		if err != nil {
			return nil, err
		}
		bs.authCtx = cloud.NewContext(*project, client)
	}
	return bs.authCtx, nil
}

// objName joins path parts into an object name.
// Unlike path.Join, it doesn't collapse ".." or strip trailing slashes.
// It also adds the value of the -gcs_backup_storage_root flag if set.
func objName(parts ...string) string {
	if *root != "" {
		return *root + "/" + strings.Join(parts, "/")
	}
	return strings.Join(parts, "/")
}

func init() {
	backupstorage.BackupStorageMap["gcs"] = &GCSBackupStorage{}
}

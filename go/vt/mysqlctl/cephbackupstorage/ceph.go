// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package cephbackupstorage implements the BackupStorage interface
// for Google Cloud Storage.
package cephbackupstorage

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/minio/minio-go"
	"github.com/youtube/vitess/go/vt/mysqlctl/backupstorage"
	"golang.org/x/net/context"
	"google.golang.org/cloud/storage"
)

var (
	// bucket is where the backups will go.
	//	bucket = flag.String("ceph_backup_storage_bucket", "", "Ceph Cloud Storage bucket to use for backups")

	bucket = "minio-bucket1"
	// root is a prefix added to all object names.
//	root = flag.String("sd", "", "root prefix for all backup-related object names")
)

// GCSBackupHandle implements BackupHandle for Google Cloud Storage.
type CephBackupHandle struct {
	client_ceph *minio.Client

	bs       *CephBackupStorage
	dir      string
	name     string
	readOnly bool
}

// Directory implements BackupHandle.
func (bh *CephBackupHandle) Directory() string {
	return bh.dir
}

// Name implements BackupHandle.
func (bh *CephBackupHandle) Name() string {
	return bh.name
}

// AddFile implements BackupHandle.
func (bh *CephBackupHandle) AddFile(filename string) (io.WriteCloser, error) {
	if bh.readOnly {
		return nil, fmt.Errorf("AddFile cannot be called on read-only backup")
	}
	object := objName(bh.dir, bh.name, filename)
	//	return bh.client_ceph.Bucket(*bucket).Object(object).NewWriter(context.TODO()), nil
	return bh.client_ceph.Bucket(bucket).Object(object).NewWriter(context.TODO()), nil
}

// EndBackup implements BackupHandle.
func (bh *CephBackupHandle) EndBackup() error {
	if bh.readOnly {
		return fmt.Errorf("EndBackup cannot be called on read-only backup")
	}
	return nil
}

// AbortBackup implements BackupHandle.
func (bh *CephBackupHandle) AbortBackup() error {
	if bh.readOnly {
		return fmt.Errorf("AbortBackup cannot be called on read-only backup")
	}
	return bh.bs.RemoveBackup(bh.dir, bh.name)
}

// ReadFile implements BackupHandle.
func (bh *CephBackupHandle) ReadFile(filename string) (io.ReadCloser, error) {
	if !bh.readOnly {
		return nil, fmt.Errorf("ReadFile cannot be called on read-write backup")
	}
	object := objName(bh.dir, bh.name, filename)
	//	return bh.client_ceph.Bucket(*bucket).Object(object).NewReader(context.TODO())
	return bh.client_ceph.Bucket(bucket).Object(object).NewReader(context.TODO())
}

// GCSBackupStorage implements BackupStorage for Google Cloud Storage.
type CephBackupStorage struct {
	// client is the instance of the Google Cloud Storage Go client.
	// Once this field is set, it must not be written again/unset to nil.
	_client     *storage.Client
	client_ceph *minio.Client

	// mu guards all fields.
	mu sync.Mutex
}

// ListBackups implements BackupStorage.
func (bs *CephBackupStorage) ListBackups(dir string) ([]backupstorage.BackupHandle, error) {
	c, err := bs.client()

	if err != nil {
		return nil, err
	}
	fmt.Println(c)

	// List prefixes that begin with dir (i.e. list subdirs).
	var subdirs []string
	fmt.Println(subdirs)
	searchPrefix := objName(dir, "" /* include trailing slash */)
	query := &minio.Query{
		Delimiter: "/",
		Prefix:    searchPrefix,
	}

	// Loop in case results are returned in multiple batches.
	for query != nil {
		//		objs, err := c.Bucket(*bucket).List(context.TODO(), query)
		objs, err := c.Bucket(bucket).List(context.TODO(), query)
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
		result = append(result, &CephBackupHandle{
			client_ceph: c,
			bs:          bs,
			dir:         dir,
			name:        subdir,
			readOnly:    true,
		})
	}
	return result, nil
}

// StartBackup implements BackupStorage.
func (bs *CephBackupStorage) StartBackup(dir, name string) (backupstorage.BackupHandle, error) {
	c, err := bs.client()
	if err != nil {
		return nil, err
	}

	return &CephBackupHandle{
		client_ceph: c,
		bs:          bs,
		dir:         dir,
		name:        name,
		readOnly:    false,
	}, nil
}

// RemoveBackup implements BackupStorage.
func (bs *CephBackupStorage) RemoveBackup(dir, name string) error {
	c, err := bs.client()
	if err != nil {
		return err
	}

	// Find all objects with the right prefix.
	query := &minio.Query{
		Prefix: objName(dir, name, "" /* include trailing slash */),
	}

	// Loop in case results are returned in multiple batches.
	for query != nil {
		//		objs, err := c.Bucket(*bucket).List(context.TODO(), query)
		objs, err := c.Bucket(bucket).List(context.TODO(), query)
		if err != nil {
			return err
		}

		// Delete all the found objects.
		for _, obj := range objs.Results {
			if err := c.Bucket(bucket).Object(obj.Name).Delete(context.TODO()); err != nil {
				//			if err := c.Bucket(*bucket).Object(obj.Name).Delete(context.TODO()); err != nil {
				//				return fmt.Errorf("unable to delete %q from bucket %q: %v", obj.Name, *bucket, err)
				return fmt.Errorf("unable to delete %q from bucket %q: %v", obj.Name, bucket, err)
			}
		}

		query = objs.Next
	}

	return nil
}

// Close implements BackupStorage.
func (bs *CephBackupStorage) Close() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if bs.client_ceph != nil {
		// If client.Close() fails, we still clear bs._client, so we know to create
		// a new client the next time one is needed.
		client := bs.client_ceph
		bs.client_ceph = nil
		if err := client.Close(); err != nil {
			return err
		}
	}
	return nil
}

// client returns the GCS Storage client instance.
// If there isn't one yet, it tries to create one.
func (bs *CephBackupStorage) client() (*minio.Client, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if bs.client_ceph == nil {
		accessKey := "439SQDG76BGBAM8ILSKR"
		secretKey := "zu7wZxwJYKUHMf7KJISKFSbvUC546Ge3KO3qVXbT"
		url := "10.47.2.3"
		ceph_client, err := minio.NewV2(url, accessKey, secretKey, true)
		if err != nil {
			return nil, err
		}
		bs.client_ceph = ceph_client
	}
	return bs.client_ceph, nil
}

// objName joins path parts into an object name.
// Unlike path.Join, it doesn't collapse ".." or strip trailing slashes.
// It also adds the value of the -ceph_backup_storage_root flag if set.
func objName(parts ...string) string {
	//	if *root != "" {
	//		return *root + "/" + strings.Join(parts, "/")
	//	}
	return strings.Join(parts, "/")
}

func init() {
	backupstorage.BackupStorageMap["ceph"] = &CephBackupStorage{}
}

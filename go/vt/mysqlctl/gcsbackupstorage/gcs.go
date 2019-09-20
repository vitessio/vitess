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

// Package gcsbackupstorage implements the BackupStorage interface
// for Google Cloud Storage.
package gcsbackupstorage

import (
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
)

var (
	_ = flag.String("gcs_backup_storage_project", "", "This flag is unused and deprecated. It will be removed entirely in a future release.")

	// bucket is where the backups will go.
	bucket = flag.String("gcs_backup_storage_bucket", "", "Google Cloud Storage bucket to use for backups")

	// root is a prefix added to all object names.
	root = flag.String("gcs_backup_storage_root", "", "root prefix for all backup-related object names")
)

// GCSBackupHandle implements BackupHandle for Google Cloud Storage.
type GCSBackupHandle struct {
	client   *storage.Client
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
func (bh *GCSBackupHandle) AddFile(ctx context.Context, filename string, filesize int64) (io.WriteCloser, error) {
	if bh.readOnly {
		return nil, fmt.Errorf("AddFile cannot be called on read-only backup")
	}
	object := objName(bh.dir, bh.name, filename)
	return bh.client.Bucket(*bucket).Object(object).NewWriter(ctx), nil
}

// EndBackup implements BackupHandle.
func (bh *GCSBackupHandle) EndBackup(ctx context.Context) error {
	if bh.readOnly {
		return fmt.Errorf("EndBackup cannot be called on read-only backup")
	}
	return nil
}

// AbortBackup implements BackupHandle.
func (bh *GCSBackupHandle) AbortBackup(ctx context.Context) error {
	if bh.readOnly {
		return fmt.Errorf("AbortBackup cannot be called on read-only backup")
	}
	return bh.bs.RemoveBackup(ctx, bh.dir, bh.name)
}

// ReadFile implements BackupHandle.
func (bh *GCSBackupHandle) ReadFile(ctx context.Context, filename string) (io.ReadCloser, error) {
	if !bh.readOnly {
		return nil, fmt.Errorf("ReadFile cannot be called on read-write backup")
	}
	object := objName(bh.dir, bh.name, filename)
	return bh.client.Bucket(*bucket).Object(object).NewReader(ctx)
}

// GCSBackupStorage implements BackupStorage for Google Cloud Storage.
type GCSBackupStorage struct {
	// client is the instance of the Google Cloud Storage Go client.
	// Once this field is set, it must not be written again/unset to nil.
	_client *storage.Client
	// mu guards all fields.
	mu sync.Mutex
}

// ListBackups implements BackupStorage.
func (bs *GCSBackupStorage) ListBackups(ctx context.Context, dir string) ([]backupstorage.BackupHandle, error) {
	c, err := bs.client(ctx)
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

	it := c.Bucket(*bucket).Objects(ctx, query)
	for {
		obj, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		// Each returned prefix is a subdir.
		// Strip parent dir from full path.
		if obj.Prefix != "" {
			subdir := strings.TrimPrefix(obj.Prefix, searchPrefix)
			subdir = strings.TrimSuffix(subdir, "/")
			subdirs = append(subdirs, subdir)
		}
	}

	// Backups must be returned in order, oldest first.
	sort.Strings(subdirs)

	result := make([]backupstorage.BackupHandle, 0, len(subdirs))
	for _, subdir := range subdirs {
		result = append(result, &GCSBackupHandle{
			client:   c,
			bs:       bs,
			dir:      dir,
			name:     subdir,
			readOnly: true,
		})
	}
	return result, nil
}

// StartBackup implements BackupStorage.
func (bs *GCSBackupStorage) StartBackup(ctx context.Context, dir, name string) (backupstorage.BackupHandle, error) {
	c, err := bs.client(ctx)
	if err != nil {
		return nil, err
	}

	return &GCSBackupHandle{
		client:   c,
		bs:       bs,
		dir:      dir,
		name:     name,
		readOnly: false,
	}, nil
}

// RemoveBackup implements BackupStorage.
func (bs *GCSBackupStorage) RemoveBackup(ctx context.Context, dir, name string) error {
	c, err := bs.client(ctx)
	if err != nil {
		return err
	}

	// Find all objects with the right prefix.
	query := &storage.Query{
		Prefix: objName(dir, name, "" /* include trailing slash */),
	}
	// Delete all the found objects.
	it := c.Bucket(*bucket).Objects(ctx, query)
	for {
		obj, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		if err := c.Bucket(*bucket).Object(obj.Name).Delete(ctx); err != nil {
			return fmt.Errorf("unable to delete %q from bucket %q: %v", obj.Name, *bucket, err)
		}
	}
	return nil
}

// Close implements BackupStorage.
func (bs *GCSBackupStorage) Close() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if bs._client != nil {
		// If client.Close() fails, we still clear bs._client,
		// so we know to create a new client the next time one
		// is needed.
		client := bs._client
		bs._client = nil
		if err := client.Close(); err != nil {
			return err
		}
	}
	return nil
}

// client returns the GCS Storage client instance.
// If there isn't one yet, it tries to create one.
func (bs *GCSBackupStorage) client(ctx context.Context) (*storage.Client, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if bs._client == nil {
		// The context needs to be valid for longer than just
		// the creation context, so we create a new one, but
		// keep the span information.
		ctx = trace.CopySpan(context.Background(), ctx)
		authClient, err := google.DefaultClient(ctx, storage.ScopeFullControl)
		if err != nil {
			return nil, err
		}
		client, err := storage.NewClient(ctx, option.WithHTTPClient(authClient))
		if err != nil {
			return nil, err
		}
		bs._client = client
	}
	return bs._client, nil
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

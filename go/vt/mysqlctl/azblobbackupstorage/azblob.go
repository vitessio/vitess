/*
Copyright 2020 The Vitess Authors.

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

// Package azblobbackupstorage implements the BackupStorage interface
// for Azure Blob Storage
package azblobbackupstorage

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
)

var (
	// This is the account name
	accountName = flag.String("azblob_backup_account_name", "", "Azure Account Name for Backups, alternative environment paramater is VITESS_AZBLOB_ACCOUNT_NAME")

	// This is the private access key
	accountKey = flag.String("azblob_backup_account_key", "", "Azure Account Key, alternative environment paramater is VITESS_AZBLOB_ACCOUNT_KEY")

	// This is the name of the container that will store the backups
	containerName = flag.String("azblob_backup_container_name", "", "Azure Blob Container Name")

	// This is an optional previx to prepend to all files
	storageRoot = flag.String("azblob_backup_storage_root", "", "Azure Blob storage root")

	azBlobParallelism = flag.Int("azblob_backup_parallelism", 1, "Azure blob operation parallelism (requires extra memory when increased)")
)

const (
	defaultRetryCount = 5
	delimiter         = "/"
)

// Return a Shared credential from the available credential sources.
// We will use credentials in the following order
// 1. Direct Command Line Flag (azblob_backup_account_name, azblob_backup_account_key)
// 2. Environment Paramaters
func azCredentials() (*azblob.SharedKeyCredential, error) {
	actName := *accountName
	if len(actName) == 0 {
		// Check the Environmental Value
		actName = os.Getenv("VITESS_AZBLOB_ACCOUNT_NAME")
	}

	actKey := *accountKey
	if len(actKey) == 0 {
		actKey = os.Getenv("VITESS_AZBLOB_ACCOUNT_NAME")
	}

	if len(actName) == 0 || len(actKey) == 0 {
		return nil, fmt.Errorf("can not get Account Credentials from CLI or Environment paramaters")
	}
	return azblob.NewSharedKeyCredential(*accountName, *accountKey)
}

func azServiceURL(credentials azblob.Credential) azblob.ServiceURL {
	pipeline := azblob.NewPipeline(credentials, azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			Policy:   azblob.RetryPolicyFixed,
			MaxTries: defaultRetryCount,
			// Per https://godoc.org/github.com/Azure/azure-storage-blob-go/azblob#RetryOptions
			// This shuld be set to a very nigh number ( they claim 60s per MB ). That could end up being days so we are limiting this to four hours
			TryTimeout: 4 * time.Hour,
		},
	})
	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/", *accountName))
	return azblob.NewServiceURL(*u, pipeline)
}

// AZBlobBackupHandle implements BackupHandle for Azure Blob service.
type AZBlobBackupHandle struct {
	bs        *AZBlobBackupStorage
	dir       string
	name      string
	readOnly  bool
	waitGroup sync.WaitGroup
	errors    concurrency.AllErrorRecorder
}

// Directory implements BackupHandle.
func (bh *AZBlobBackupHandle) Directory() string {
	return bh.dir
}

// Name implements BackupHandle.
func (bh *AZBlobBackupHandle) Name() string {
	return bh.name
}

// AddFile implements BackupHandle.
func (bh *AZBlobBackupHandle) AddFile(ctx context.Context, filename string, filesize int64) (io.WriteCloser, error) {
	if bh.readOnly {
		return nil, fmt.Errorf("AddFile cannot be called on read-only backup")
	}
	// Error out if the file size it too large ( ~4.75 TB)
	if filesize > azblob.BlockBlobMaxStageBlockBytes*azblob.BlockBlobMaxBlocks {
		return nil, fmt.Errorf("filesize is too large to upload to az blob (max size %v)", azblob.BlockBlobMaxStageBlockBytes*azblob.BlockBlobMaxBlocks)
	}

	obj := objName(bh.dir, bh.name, filename)
	containerURL, err := bh.bs.containerURL()
	if err != nil {
		return nil, err
	}

	blockBlobURL := containerURL.NewBlockBlobURL(obj)

	reader, writer := io.Pipe()
	bh.waitGroup.Add(1)

	go func() {
		defer bh.waitGroup.Done()
		_, err := azblob.UploadStreamToBlockBlob(ctx, reader, blockBlobURL, azblob.UploadStreamToBlockBlobOptions{
			BufferSize: azblob.BlockBlobMaxStageBlockBytes,
			MaxBuffers: *azBlobParallelism,
		})
		if err != nil {
			reader.CloseWithError(err)
			bh.errors.RecordError(err)
		}
	}()

	return writer, nil
}

// EndBackup implements BackupHandle.
func (bh *AZBlobBackupHandle) EndBackup(ctx context.Context) error {
	if bh.readOnly {
		return fmt.Errorf("EndBackup cannot be called on read-only backup")
	}
	bh.waitGroup.Wait()
	return bh.errors.Error()
}

// AbortBackup implements BackupHandle.
func (bh *AZBlobBackupHandle) AbortBackup(ctx context.Context) error {
	if bh.readOnly {
		return fmt.Errorf("AbortBackup cannot be called on read-only backup")
	}
	return bh.bs.RemoveBackup(ctx, bh.dir, bh.name)
}

// ReadFile implements BackupHandle.
func (bh *AZBlobBackupHandle) ReadFile(ctx context.Context, filename string) (io.ReadCloser, error) {
	if !bh.readOnly {
		return nil, fmt.Errorf("ReadFile cannot be called on read-write backup")
	}

	obj := objName(bh.dir, filename)
	containerURL, err := bh.bs.containerURL()
	if err != nil {
		return nil, err
	}
	blobURL := containerURL.NewBlobURL(obj)

	resp, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return nil, err
	}
	return resp.Body(azblob.RetryReaderOptions{
		MaxRetryRequests: defaultRetryCount,
		NotifyFailedRead: func(failureCount int, lastError error, offset int64, count int64, willRetry bool) {
			log.Infof("ReadFile: [azblob] container: %s, directory: %s, filename: %s, error: %v", *containerName, objName(bh.dir, ""), filename, lastError)
			if !willRetry {
				bh.errors.RecordError(lastError)
			}
		},
		TreatEarlyCloseAsError: true,
	}), nil
}

// AZBlobBackupStorage structs implements the BackupStorage interface for AZBlob
type AZBlobBackupStorage struct {
}

func (bs *AZBlobBackupStorage) containerURL() (*azblob.ContainerURL, error) {
	credentials, err := azCredentials()
	if err != nil {
		return nil, err
	}
	u := azServiceURL(credentials).NewContainerURL(*containerName)
	return &u, nil
}

// ListBackups implements BackupStorage.
func (bs *AZBlobBackupStorage) ListBackups(ctx context.Context, dir string) ([]backupstorage.BackupHandle, error) {
	log.Infof("ListBackups: [azblob] container: %s, directory: %v", *containerName, objName(dir, ""))

	containerURL, err := bs.containerURL()
	if err != nil {
		return nil, err
	}

	searchPrefix := objName(dir, "")

	result := make([]backupstorage.BackupHandle, 0)
	var subdirs []string

	for marker := (azblob.Marker{}); marker.NotDone(); {
		// This returns Blobs in sorted order so we don't need to sort them a second time
		resp, err := containerURL.ListBlobsHierarchySegment(ctx, marker, delimiter, azblob.ListBlobsSegmentOptions{
			Prefix:     searchPrefix,
			MaxResults: 0,
		})

		if err != nil {
			return nil, err
		}

		for _, item := range resp.Segment.BlobPrefixes {
			subdir := strings.TrimPrefix(item.Name, searchPrefix)
			subdir = strings.TrimSuffix(subdir, delimiter)
			subdirs = append(subdirs, subdir)
		}

		marker = resp.NextMarker
	}

	for _, subdir := range subdirs {
		result = append(result, &AZBlobBackupHandle{
			bs:       bs,
			dir:      dir,
			name:     subdir,
			readOnly: true,
		})
	}

	return result, nil
}

// StartBackup implements BackupStorage.
func (bs *AZBlobBackupStorage) StartBackup(ctx context.Context, dir, name string) (backupstorage.BackupHandle, error) {
	return &AZBlobBackupHandle{
		bs:       bs,
		dir:      dir,
		name:     name,
		readOnly: false,
	}, nil
}

// RemoveBackup implements BackupStorage.
func (bs *AZBlobBackupStorage) RemoveBackup(ctx context.Context, dir, name string) error {
	log.Infof("ListBackups: [azblob] container: %s, directory: %s", *containerName, objName(dir, ""))

	containerURL, err := bs.containerURL()
	if err != nil {
		return err
	}

	searchPrefix := objName(dir, name, "")

	for marker := (azblob.Marker{}); marker.NotDone(); {
		resp, err := containerURL.ListBlobsHierarchySegment(ctx, marker, delimiter, azblob.ListBlobsSegmentOptions{
			Prefix:     searchPrefix,
			MaxResults: 0,
		})

		if err != nil {
			return err
		}

		// Right now there is no batch delete so we must iterate over all the blobs to delete them one by one
		// One day we will be able to use this https://docs.microsoft.com/en-us/rest/api/storageservices/blob-batch
		// but currently it is listed as a preview and its not in the go API
		for _, item := range resp.Segment.BlobItems {
			_, err = containerURL.NewBlobURL(item.Name).Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
			if err != nil {
				return err
			}
		}
		marker = resp.NextMarker
	}

	return nil
}

// Close implements BackupStorage.
func (bs *AZBlobBackupStorage) Close() error {
	// This function is a No-op
	return nil
}

// objName joins path parts into an object name.
// Unlike path.Join, it doesn't collapse ".." or strip trailing slashes.
// It also adds the value of the -azblob_backup_storage_root flag if set.
func objName(parts ...string) string {
	if *storageRoot != "" {
		return *storageRoot + "/" + strings.Join(parts, "/")
	}
	return strings.Join(parts, "/")
}

func init() {
	backupstorage.BackupStorageMap["azblob"] = &AZBlobBackupStorage{}
}

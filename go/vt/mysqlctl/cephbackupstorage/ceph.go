// Package cephbackupstorage implements the BackupStorage interface
// for Ceph Cloud Storage.
package cephbackupstorage

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"

	"errors"

	log "github.com/golang/glog"
	minio "github.com/minio/minio-go"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/mysqlctl/backupstorage"
)

var (
	// configFilePath is where the configs/credentials for backups will be stored.
	configFilePath = flag.String("ceph_backup_storage_config", "ceph_backup_config.json",
		"Path to JSON config file for ceph backup storage")
)

var storageConfig struct {
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
	EndPoint  string `json:"endPoint"`
	UseSSL    bool   `json:"useSSL"`
}

// CephBackupHandle implements BackupHandle for Ceph Cloud Storage.
type CephBackupHandle struct {
	client    *minio.Client
	bs        *CephBackupStorage
	dir       string
	name      string
	readOnly  bool
	errors    concurrency.AllErrorRecorder
	waitGroup sync.WaitGroup
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
func (bh *CephBackupHandle) AddFile(ctx context.Context, filename string) (io.WriteCloser, error) {
	if bh.readOnly {
		return nil, fmt.Errorf("AddFile cannot be called on read-only backup")
	}
	reader, writer := io.Pipe()
	bh.waitGroup.Add(1)
	go func() {
		defer bh.waitGroup.Done()

		// ceph bucket name is where the backups will go
		//backup handle dir field contains keyspace/shard value
		bucket := alterBucketName(bh.dir)

		// Give PutObject() the read end of the pipe.
		object := objName(bh.dir, bh.name, filename)
		_, err := bh.client.PutObject(bucket, object, reader, "application/octet-stream")
		if err != nil {
			// Signal the writer that an error occurred, in case it's not done writing yet.
			reader.CloseWithError(err)
			// In case the error happened after the writer finished, we need to remember it.
			bh.errors.RecordError(err)
		}
	}()
	// Give our caller the write end of the pipe.
	return writer, nil
}

// EndBackup implements BackupHandle.
func (bh *CephBackupHandle) EndBackup(ctx context.Context) error {
	if bh.readOnly {
		return fmt.Errorf("EndBackup cannot be called on read-only backup")
	}
	bh.waitGroup.Wait()
	// Return the saved PutObject() errors, if any.
	return bh.errors.Error()
}

// AbortBackup implements BackupHandle.
func (bh *CephBackupHandle) AbortBackup(ctx context.Context) error {
	if bh.readOnly {
		return fmt.Errorf("AbortBackup cannot be called on read-only backup")
	}
	return bh.bs.RemoveBackup(ctx, bh.dir, bh.name)
}

// ReadFile implements BackupHandle.
func (bh *CephBackupHandle) ReadFile(ctx context.Context, filename string) (io.ReadCloser, error) {
	if !bh.readOnly {
		return nil, fmt.Errorf("ReadFile cannot be called on read-write backup")
	}
	// ceph bucket name
	bucket := alterBucketName(bh.dir)
	object := objName(bh.dir, bh.name, filename)
	return bh.client.GetObject(bucket, object)
}

// CephBackupStorage implements BackupStorage for Ceph Cloud Storage.
type CephBackupStorage struct {
	// client is the instance of the Ceph Cloud Storage Go client.
	// Once this field is set, it must not be written again/unset to nil.
	_client *minio.Client
	// mu guards all fields.
	mu sync.Mutex
}

// ListBackups implements BackupStorage.
func (bs *CephBackupStorage) ListBackups(ctx context.Context, dir string) ([]backupstorage.BackupHandle, error) {
	c, err := bs.client()
	if err != nil {
		return nil, err
	}
	// ceph bucket name
	bucket := alterBucketName(dir)

	// List prefixes that begin with dir (i.e. list subdirs).
	var subdirs []string
	searchPrefix := objName(dir, "")

	doneCh := make(chan struct{})
	for object := range c.ListObjects(bucket, searchPrefix, false, doneCh) {
		if object.Err != nil {
			err := c.BucketExists(bucket)
			if err != nil {
				return nil, nil
			}
			return nil, object.Err
		}
		subdir := strings.TrimPrefix(object.Key, searchPrefix)
		subdir = strings.TrimSuffix(subdir, "/")
		subdirs = append(subdirs, subdir)
	}

	// Backups must be returned in order, oldest first.
	sort.Strings(subdirs)

	result := make([]backupstorage.BackupHandle, 0, len(subdirs))
	for _, subdir := range subdirs {
		result = append(result, &CephBackupHandle{
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
func (bs *CephBackupStorage) StartBackup(ctx context.Context, dir, name string) (backupstorage.BackupHandle, error) {
	c, err := bs.client()
	if err != nil {
		return nil, err
	}
	// ceph bucket name
	bucket := alterBucketName(dir)

	err = c.BucketExists(bucket)
	if err != nil {
		log.Info("Bucket: %v doesn't exist, creating new bucket with the required name", bucket)
		err = c.MakeBucket(bucket, "")
		if err != nil {
			log.Info("Error creating Bucket: %v, quitting", bucket)
			return nil, errors.New("Error creating new bucket: " + bucket)
		}
	}

	return &CephBackupHandle{
		client:   c,
		bs:       bs,
		dir:      dir,
		name:     name,
		readOnly: false,
	}, nil
}

// RemoveBackup implements BackupStorage.
func (bs *CephBackupStorage) RemoveBackup(ctx context.Context, dir, name string) error {
	c, err := bs.client()
	if err != nil {
		return err
	}
	// ceph bucket name
	bucket := alterBucketName(dir)

	fullName := objName(dir, name, "")
	var arr []string
	doneCh := make(chan struct{})
	defer close(doneCh)
	for object := range c.ListObjects(bucket, fullName, true, doneCh) {
		if object.Err != nil {
			return object.Err
		}
		arr = append(arr, object.Key)
	}
	for _, obj := range arr {
		err = c.RemoveObject(bucket, obj)
		if err != nil {
			return err
		}
	}
	return nil
}

// Close implements BackupStorage.
func (bs *CephBackupStorage) Close() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if bs._client != nil {
		// a new client the next time one is needed.
		bs._client = nil
	}
	return nil
}

// client returns the Ceph Storage client instance.
// If there isn't one yet, it tries to create one.
func (bs *CephBackupStorage) client() (*minio.Client, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if bs._client == nil {
		configFile, err := os.Open(*configFilePath)
		if err != nil {
			return nil, fmt.Errorf("file not present : %v", err)
		}
		defer configFile.Close()
		jsonParser := json.NewDecoder(configFile)
		if err = jsonParser.Decode(&storageConfig); err != nil {
			return nil, fmt.Errorf("Error parsing the json file : %v", err)
		}

		accessKey := storageConfig.AccessKey
		secretKey := storageConfig.SecretKey
		url := storageConfig.EndPoint
		useSSL := storageConfig.UseSSL

		client, err := minio.NewV2(url, accessKey, secretKey, useSSL)
		if err != nil {
			return nil, err
		}
		bs._client = client
	}
	return bs._client, nil
}

func init() {
	backupstorage.BackupStorageMap["ceph"] = &CephBackupStorage{}
}

// objName joins path parts into an object name.
// Unlike path.Join, it doesn't collapse ".." or strip trailing slashes.
// It also adds the value of the -gcs_backup_storage_root flag if set.
func objName(parts ...string) string {
	return strings.Join(parts, "/")
}

// keeping in view the bucket naming conventions for ceph
// only keyspace informations is extracted and used for bucket name
func alterBucketName(dir string) string {
	bucket := strings.ToLower(dir)
	bucket = strings.Split(bucket, "/")[0]
	bucket = strings.Replace(bucket, "_", "-", -1)
	return bucket
}

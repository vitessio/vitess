// Package cephbackupstorage implements the BackupStorage interface
// for Ceph Cloud Storage.
package cephbackupstorage

import (
	"fmt"
	"io"
	"log"
	"strings"
	"sync"

	log1 "github.com/golang/glog"

	"github.com/minio/minio-go"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/mysqlctl/backupstorage"
	//	"golang.org/x/net/context"
)

var (
	// bucket is where the backups will go.
	//	bucket = flag.String("ceph_backup_storage_bucket", "", "Ceph Cloud Storage bucket to use for backups")

	//	bucket = "raunaktestbucket"
	bucket = "minio-bucket1"
	// root is a prefix added to all object names.
//	root = flag.String("sd", "", "root prefix for all backup-related object names")
)

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
func (bh *CephBackupHandle) AddFile(filename string) (io.WriteCloser, error) {
	if bh.readOnly {
		return nil, fmt.Errorf("AddFile cannot be called on read-only backup")
	}
	//	Anthony's Code
	// Pipe whatever the backup system gives us into PutObject().
	reader, writer := io.Pipe()
	bh.waitGroup.Add(1)
	go func() {
		defer bh.waitGroup.Done()
		// Give PutObject() the read end of the pipe.
		fmt.Println(bucket, filename)
		log1.Info(bucket, filename)
		_, err := bh.client.PutObject(bucket, filename, reader, "application/octet-stream")
		if err != nil {
			fmt.Println("Error with io.Pipe")
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
func (bh *CephBackupHandle) EndBackup() error {
	//	if bh.readOnly {
	//		return fmt.Errorf("EndBackup cannot be called on read-only backup")
	//	}
	//	return nil

	// Wait for goroutines launched by AddFile() to finish, so we don't miss any errors.
	bh.waitGroup.Wait()
	// Return the saved PutObject() errors, if any.
	return bh.errors.Error()
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
	//	object := objName(bh.dir, bh.name, filename)
	//	//	return bh.client_ceph.Bucket(*bucket).Object(object).NewReader(context.TODO())
	//	return bh.client.Bucket(bucket).Object(object).NewReader(context.TODO())

	/*
		reader, writer := io.Pipe()
		//	var obj *minio.Object
		//	var err error
		bh.waitGroup.Add(1)
		go func() {
			defer bh.waitGroup.Done()
			// Give PutObject() the read end of the pipe.
			obj, err := bh.client.GetObject(bucket, filename)
			stat, err := obj.Stat()
			if _, err = io.CopyN(writer, reader, stat.Size); err != nil {
				log.Fatalln(err)
			}
			if err != nil {
				log.Fatalln(err)
			}

			if err != nil {
				fmt.Println("Error with io.Pipe")
				// Signal the writer that an error occurred, in case it's not done writing yet.
				reader.CloseWithError(err)
				// In case the error happened after the writer finished, we need to remember it.
				bh.errors.RecordError(err)
			}
		}()

		// Give our caller the write end of the pipe.
		return reader, nil

	*/
	//	return bh.client.GetObject(bucket, objName(bh.dir, bh.name, filename))
	return bh.client.GetObject(bucket, filename)
}

// GCSBackupStorage implements BackupStorage for Google Cloud Storage.
type CephBackupStorage struct {
	// client is the instance of the Minio Go client.
	// Once this field is set, it must not be written again/unset to nil.
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

	doneCh := make(chan struct{})
	// Loop in case results are returned in multiple batches.
	dirTemp := dir
	dir = "minio-bucket1"
	var result []backupstorage.BackupHandle
	for object := range c.ListObjects(dir, "", false, doneCh) {
		if object.Err != nil {
			return nil, object.Err
		}
		result = append(result, &CephBackupHandle{
			client:   c,
			bs:       bs,
			dir:      dirTemp,
			name:     object.Key,
			readOnly: true,
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
	dir = "minio-bucket1"

	return &CephBackupHandle{
		client:   c,
		bs:       bs,
		dir:      dir,
		name:     name,
		readOnly: false,
	}, nil
}

// RemoveBackup implements BackupStorage.
func (bs *CephBackupStorage) RemoveBackup(dir, name string) error {
	c, err := bs.client()
	if err != nil {
		return err
	}
	dir = "minio-bucket1"
	err = c.RemoveObject(dir, name)
	if err != nil {
		log.Fatalln(err)
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

// client returns the Ceph Storage client instance.
// If there isn't one yet, it tries to create one.
func (bs *CephBackupStorage) client() (*minio.Client, error) {
	//	client resource has been locked such that no other instance/thread can edit the client object
	//	single occurance
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

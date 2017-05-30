/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package s3backupstorage implements the BackupStorage interface for AWS S3.
//
// AWS access credentials are configured via standard AWS means, such as:
// - AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables
// - credentials file at ~/.aws/credentials
// - if running on an EC2 instance, an IAM role
// See details at http://blogs.aws.amazon.com/security/post/Tx3D6U6WSFGOK2H/A-New-and-Standardized-Way-to-Manage-Credentials-in-the-AWS-SDKs
package s3backupstorage

import (
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/mysqlctl/backupstorage"
)

var (
	// AWS API region
	region = flag.String("s3_backup_aws_region", "us-east-1", "AWS region to use")

	// bucket is where the backups will go.
	bucket = flag.String("s3_backup_storage_bucket", "", "S3 bucket to use for backups")

	// root is a prefix added to all object names.
	root = flag.String("s3_backup_storage_root", "", "root prefix for all backup-related object names")

	// path component delimiter
	delimiter = "/"
)

// S3BackupHandle implements the backupstorage.BackupHandle interface.
type S3BackupHandle struct {
	client    *s3.S3
	bs        *S3BackupStorage
	dir       string
	name      string
	readOnly  bool
	errors    concurrency.AllErrorRecorder
	waitGroup sync.WaitGroup
}

// Directory is part of the backupstorage.BackupHandle interface.
func (bh *S3BackupHandle) Directory() string {
	return bh.dir
}

// Name is part of the backupstorage.BackupHandle interface.
func (bh *S3BackupHandle) Name() string {
	return bh.name
}

// AddFile is part of the backupstorage.BackupHandle interface.
func (bh *S3BackupHandle) AddFile(ctx context.Context, filename string) (io.WriteCloser, error) {
	if bh.readOnly {
		return nil, fmt.Errorf("AddFile cannot be called on read-only backup")
	}

	reader, writer := io.Pipe()
	bh.waitGroup.Add(1)

	go func() {
		defer bh.waitGroup.Done()
		uploader := s3manager.NewUploaderWithClient(bh.client)
		object := objName(bh.dir, bh.name, filename)
		_, err := uploader.Upload(&s3manager.UploadInput{
			Bucket: bucket,
			Key:    object,
			Body:   reader,
		})
		if err != nil {
			reader.CloseWithError(err)
			bh.errors.RecordError(err)
		}
	}()

	return writer, nil
}

// EndBackup is part of the backupstorage.BackupHandle interface.
func (bh *S3BackupHandle) EndBackup(ctx context.Context) error {
	if bh.readOnly {
		return fmt.Errorf("EndBackup cannot be called on read-only backup")
	}
	bh.waitGroup.Wait()
	return bh.errors.Error()
}

// AbortBackup is part of the backupstorage.BackupHandle interface.
func (bh *S3BackupHandle) AbortBackup(ctx context.Context) error {
	if bh.readOnly {
		return fmt.Errorf("AbortBackup cannot be called on read-only backup")
	}
	return bh.bs.RemoveBackup(ctx, bh.dir, bh.name)
}

// ReadFile is part of the backupstorage.BackupHandle interface.
func (bh *S3BackupHandle) ReadFile(ctx context.Context, filename string) (io.ReadCloser, error) {
	if !bh.readOnly {
		return nil, fmt.Errorf("ReadFile cannot be called on read-write backup")
	}
	object := objName(bh.dir, bh.name, filename)
	out, err := bh.client.GetObject(&s3.GetObjectInput{
		Bucket: bucket,
		Key:    object,
	})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

var _ backupstorage.BackupHandle = (*S3BackupHandle)(nil)

// S3BackupStorage implements the backupstorage.BackupStorage interface.
type S3BackupStorage struct {
	_client *s3.S3
	mu      sync.Mutex
}

// ListBackups is part of the backupstorage.BackupStorage interface.
func (bs *S3BackupStorage) ListBackups(ctx context.Context, dir string) ([]backupstorage.BackupHandle, error) {
	c, err := bs.client()
	if err != nil {
		return nil, err
	}

	searchPrefix := objName(dir, "")
	query := &s3.ListObjectsV2Input{
		Bucket:    bucket,
		Delimiter: &delimiter,
		Prefix:    searchPrefix,
	}

	var subdirs []string
	for {
		objs, err := c.ListObjectsV2(query)
		if err != nil {
			return nil, err
		}
		for _, prefix := range objs.CommonPrefixes {
			subdir := strings.TrimPrefix(*prefix.Prefix, *searchPrefix)
			subdir = strings.TrimSuffix(subdir, delimiter)
			subdirs = append(subdirs, subdir)
		}

		if objs.NextContinuationToken == nil {
			break
		}
		query.ContinuationToken = objs.NextContinuationToken
	}

	// Backups must be returned in order, oldest first.
	sort.Strings(subdirs)

	result := make([]backupstorage.BackupHandle, 0, len(subdirs))
	for _, subdir := range subdirs {
		result = append(result, &S3BackupHandle{
			client:   c,
			bs:       bs,
			dir:      dir,
			name:     subdir,
			readOnly: true,
		})
	}
	return result, nil
}

// StartBackup is part of the backupstorage.BackupStorage interface.
func (bs *S3BackupStorage) StartBackup(ctx context.Context, dir, name string) (backupstorage.BackupHandle, error) {
	c, err := bs.client()
	if err != nil {
		return nil, err
	}

	return &S3BackupHandle{
		client:   c,
		bs:       bs,
		dir:      dir,
		name:     name,
		readOnly: false,
	}, nil
}

// RemoveBackup is part of the backupstorage.BackupStorage interface.
func (bs *S3BackupStorage) RemoveBackup(ctx context.Context, dir, name string) error {
	c, err := bs.client()
	if err != nil {
		return err
	}

	query := &s3.ListObjectsV2Input{
		Bucket: bucket,
		Prefix: objName(dir, name),
	}

	for {
		objs, err := c.ListObjectsV2(query)
		if err != nil {
			return err
		}

		objIds := make([]*s3.ObjectIdentifier, 0, len(objs.Contents))
		for _, obj := range objs.Contents {
			objIds = append(objIds, &s3.ObjectIdentifier{
				Key: obj.Key,
			})
		}

		quiet := true // return less in the Delete response
		out, err := c.DeleteObjects(&s3.DeleteObjectsInput{
			Bucket: bucket,
			Delete: &s3.Delete{
				Objects: objIds,
				Quiet:   &quiet,
			},
		})

		if err != nil {
			return err
		}

		for _, objError := range out.Errors {
			return fmt.Errorf(objError.String())
		}

		if objs.NextContinuationToken == nil {
			break
		}

		query.ContinuationToken = objs.NextContinuationToken
	}

	return nil
}

// Close is part of the backupstorage.BackupStorage interface.
func (bs *S3BackupStorage) Close() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	bs._client = nil
	return nil
}

var _ backupstorage.BackupStorage = (*S3BackupStorage)(nil)

func (bs *S3BackupStorage) client() (*s3.S3, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	if bs._client == nil {
		bs._client = s3.New(session.New(), &aws.Config{Region: aws.String(*region)})

		if len(*bucket) == 0 {
			return nil, fmt.Errorf("-s3_backup_storage_bucket required")
		}

		if _, err := bs._client.HeadBucket(&s3.HeadBucketInput{Bucket: bucket}); err != nil {
			return nil, err
		}
	}
	return bs._client, nil
}

func objName(parts ...string) *string {
	res := ""
	if *root != "" {
		res += *root + delimiter
	}
	res += strings.Join(parts, delimiter)
	return &res
}

func init() {
	backupstorage.BackupStorageMap["s3"] = &S3BackupStorage{}
}

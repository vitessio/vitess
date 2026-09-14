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

// Package cephbackupstorage implements the BackupStorage interface
// for Ceph Cloud Storage.
package cephbackupstorage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	errorsbackup "vitess.io/vitess/go/vt/mysqlctl/errors"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/utils"
)

// configFilePath is where the configs/credentials for backups will be stored.
var configFilePath string

func registerFlags(fs *pflag.FlagSet) {
	utils.SetFlagStringVar(fs, &configFilePath, "ceph-backup-storage-config", "ceph_backup_config.json",
		"Path to JSON config file for ceph backup storage.")
}

func init() {
	servenv.OnParseFor("vtbackup", registerFlags)
	servenv.OnParseFor("vtctl", registerFlags)
	servenv.OnParseFor("vtctld", registerFlags)
	servenv.OnParseFor("vttablet", registerFlags)
}

// storageConfig is the content of the JSON file named by --ceph-backup-storage-config.
type storageConfig struct {
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
	// EndPoint is the gateway's host:port, without a scheme.
	EndPoint string `json:"endPoint"`
	UseSSL   bool   `json:"useSSL"`
}

// cephRegion is the region the S3 client is configured with. Ceph RGW ignores
// it for signing purposes, but the AWS SDK requires one.
const cephRegion = "us-east-1"

// CephBackupHandle implements BackupHandle for Ceph Cloud Storage.
type CephBackupHandle struct {
	client    *s3.Client
	bs        *CephBackupStorage
	dir       string
	name      string
	readOnly  bool
	waitGroup sync.WaitGroup
	errorsbackup.PerFileErrorRecorder
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
func (bh *CephBackupHandle) AddFile(ctx context.Context, filename string, filesize int64) (io.WriteCloser, error) {
	if bh.readOnly {
		return nil, errors.New("AddFile cannot be called on read-only backup")
	}
	reader, writer := io.Pipe()
	bh.waitGroup.Go(func() {
		// ceph bucket name is where the backups will go
		// backup handle dir field contains keyspace/shard value
		bucket := alterBucketName(bh.dir)

		// Give PutObject() the read end of the pipe.
		object := objName(bh.dir, bh.name, filename)
		// The body is a pipe, which smithy always sends with chunked
		// transfer encoding and no Content-Length, so filesize is
		// deliberately not passed as ContentLength: the signer would include
		// a content-length header in the signature that never reaches the
		// wire, and the gateway would reject the request with
		// SignatureDoesNotMatch.
		_, err := bh.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(object),
			Body:        reader,
			ContentType: aws.String("application/octet-stream"),
		})
		if err != nil {
			// Signal the writer that an error occurred, in case it's not done writing yet.
			reader.CloseWithError(err)
			// In case the error happened after the writer finished, we need to remember it.
			bh.RecordError(filename, err)
		}
	})
	// Give our caller the write end of the pipe.
	return writer, nil
}

// Wait implements BackupHandle.
func (bh *CephBackupHandle) Wait() {
	bh.waitGroup.Wait()
}

// EndBackup implements BackupHandle.
func (bh *CephBackupHandle) EndBackup(ctx context.Context) error {
	if bh.readOnly {
		return errors.New("EndBackup cannot be called on read-only backup")
	}
	bh.Wait()
	// Return the saved PutObject() errors, if any.
	return bh.Error()
}

// AbortBackup implements BackupHandle.
func (bh *CephBackupHandle) AbortBackup(ctx context.Context) error {
	if bh.readOnly {
		return errors.New("AbortBackup cannot be called on read-only backup")
	}
	return bh.bs.RemoveBackup(ctx, bh.dir, bh.name)
}

// ReadFile implements BackupHandle.
func (bh *CephBackupHandle) ReadFile(ctx context.Context, filename string) (io.ReadCloser, error) {
	if !bh.readOnly {
		return nil, errors.New("ReadFile cannot be called on read-write backup")
	}
	// ceph bucket name
	bucket := alterBucketName(bh.dir)
	object := objName(bh.dir, bh.name, filename)
	out, err := bh.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

// CephBackupStorage implements BackupStorage for Ceph Cloud Storage.
type CephBackupStorage struct {
	// _client is the instance of the S3 client used to talk to Ceph.
	_client *s3.Client
	// config is the decoded configuration. Tests set it directly; production
	// loads it from configFilePath the first time a client is needed and drops
	// it again in Close, so the file is re-read on the next use.
	config *storageConfig
	// configFromFile records that config came from configFilePath rather than
	// from a test, so Close knows whether to drop it.
	configFromFile bool
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

	paginator := s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(searchPrefix),
		Delimiter: aws.String("/"),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			if isNoSuchBucket(err) {
				return nil, nil
			}
			return nil, err
		}
		for _, cp := range page.CommonPrefixes {
			subdir := strings.TrimPrefix(aws.ToString(cp.Prefix), searchPrefix)
			subdir = strings.TrimSuffix(subdir, "/")
			subdirs = append(subdirs, subdir)
		}
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

	found, err := bucketExists(ctx, c, bucket)
	if err != nil {
		log.Info("Error checking whether bucket exists", slog.String("bucket", bucket), slog.Any("error", err))
		return nil, errors.New("Error checking whether bucket exists: " + bucket)
	}
	if !found {
		log.Info(fmt.Sprintf("Bucket: %v doesn't exist, creating new bucket with the required name", bucket))
		_, err = c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
		if err != nil {
			log.Info("Error creating bucket", slog.String("bucket", bucket), slog.Any("error", err))
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
	paginator := s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(fullName),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return err
		}
		for _, obj := range page.Contents {
			arr = append(arr, aws.ToString(obj.Key))
		}
	}
	for _, obj := range arr {
		_, err = c.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(obj),
		})
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

	// Drop the client so a new one is built the next time one is needed.
	bs._client = nil
	if bs.configFromFile {
		// Drop the config too, so the file is re-read along with it.
		bs.config = nil
	}
	return nil
}

func (bs *CephBackupStorage) WithParams(params backupstorage.Params) backupstorage.BackupStorage {
	// TODO(maxeng): return a new CephBackupStorage that uses params.
	return bs
}

// loadConfigLocked reads configFilePath into bs.config unless a config is
// already present. bs.mu must be held.
func (bs *CephBackupStorage) loadConfigLocked() error {
	if bs.config != nil {
		return nil
	}
	configFile, err := os.Open(configFilePath)
	if err != nil {
		return fmt.Errorf("file not present : %v", err)
	}
	defer configFile.Close()
	var cfg storageConfig
	if err = json.NewDecoder(configFile).Decode(&cfg); err != nil {
		return fmt.Errorf("error parsing the json file : %v", err)
	}
	bs.config = &cfg
	bs.configFromFile = true
	return nil
}

// endpointURL returns the configured endpoint with the scheme UseSSL selects.
// bs.mu must be held, or bs.config must be immutable (as it is for a
// test-supplied config).
func (bs *CephBackupStorage) endpointURL() string {
	scheme := "http"
	if bs.config.UseSSL {
		scheme = "https"
	}
	return scheme + "://" + bs.config.EndPoint
}

// client returns the Ceph Storage client instance.
// If there isn't one yet, it tries to create one.
func (bs *CephBackupStorage) client() (*s3.Client, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if bs._client == nil {
		if err := bs.loadConfigLocked(); err != nil {
			return nil, err
		}
		cfg, err := config.LoadDefaultConfig(context.Background(),
			config.WithRegion(cephRegion),
			config.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(bs.config.AccessKey, bs.config.SecretKey, "")),
		)
		if err != nil {
			return nil, err
		}
		endpoint := bs.endpointURL()
		bs._client = s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
			// AddFile streams each file through an io.Pipe, which cannot be
			// rewound, so the SDK cannot read the payload to hash it for the
			// signature or for a checksum header. Skip the default CRC32
			// checksum (otherwise the SDK rejects an unseekable body over
			// plain HTTP) and sign the headers only, sending the payload as
			// UNSIGNED-PAYLOAD. This applies over HTTPS as well: with the
			// checksum disabled the SDK does not fall back to a trailing
			// checksum there either. It matches what minio-go did, since
			// Signature V2 never covered the payload.
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
			// Symmetric with the request side: nothing here writes checksums,
			// so there is nothing to validate, and the SDK would otherwise
			// warn on every download.
			o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
			o.APIOptions = append(o.APIOptions, v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware)
		})
	}
	return bs._client, nil
}

// bucketExists reports whether bucket exists, distinguishing "not found"
// from other errors.
func bucketExists(ctx context.Context, c *s3.Client, bucket string) (bool, error) {
	_, err := c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err == nil {
		return true, nil
	}
	if isNoSuchBucket(err) {
		return false, nil
	}
	return false, err
}

// isNoSuchBucket reports whether err says the bucket does not exist. Listing
// a missing bucket returns NoSuchBucket, while HeadBucket returns the generic
// NotFound because a HEAD response has no body to name the error.
func isNoSuchBucket(err error) bool {
	var noSuchBucket *types.NoSuchBucket
	var notFound *types.NotFound
	return errors.As(err, &noSuchBucket) || errors.As(err, &notFound)
}

func init() {
	backupstorage.BackupStorageMap["ceph"] = &CephBackupStorage{}
}

// objName joins path parts into an object name.
// Unlike path.Join, it doesn't collapse ".." or strip trailing slashes.
func objName(parts ...string) string {
	return strings.Join(parts, "/")
}

// keeping in view the bucket naming conventions for ceph
// only keyspace informations is extracted and used for bucket name
func alterBucketName(dir string) string {
	bucket := strings.ToLower(dir)
	bucket = strings.Split(bucket, "/")[0]
	bucket = strings.ReplaceAll(bucket, "_", "-")
	return bucket
}

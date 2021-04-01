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

// Package s3backupstorage implements the BackupStorage interface for AWS S3.
//
// AWS access credentials are configured via standard AWS means, such as:
// - AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables
// - credentials file at ~/.aws/credentials
// - if running on an EC2 instance, an IAM role
// See details at http://blogs.aws.amazon.com/security/post/Tx3D6U6WSFGOK2H/A-New-and-Standardized-Way-to-Manage-Credentials-in-the-AWS-SDKs
package s3backupstorage

import (
	"crypto/md5"
	"crypto/tls"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"

	"vitess.io/vitess/go/vt/log"

	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
)

var (
	// AWS API region
	region = flag.String("s3_backup_aws_region", "us-east-1", "AWS region to use")

	// AWS request retries
	retryCount = flag.Int("s3_backup_aws_retries", -1, "AWS request retries")

	// AWS endpoint, defaults to amazonaws.com but appliances may use a different location
	endpoint = flag.String("s3_backup_aws_endpoint", "", "endpoint of the S3 backend (region must be provided)")

	// bucket is where the backups will go.
	bucket = flag.String("s3_backup_storage_bucket", "", "S3 bucket to use for backups")

	// root is a prefix added to all object names.
	root = flag.String("s3_backup_storage_root", "", "root prefix for all backup-related object names")

	// forcePath is used to ensure that the certificate and path used match the endpoint + region
	forcePath = flag.Bool("s3_backup_force_path_style", false, "force the s3 path style")

	tlsSkipVerifyCert = flag.Bool("s3_backup_tls_skip_verify_cert", false, "skip the 'certificate is valid' check for SSL connections")

	// verboseLogging provides more verbose logging of AWS actions
	requiredLogLevel = flag.String("s3_backup_log_level", "LogOff", "determine the S3 loglevel to use from LogOff, LogDebug, LogDebugWithSigning, LogDebugWithHTTPBody, LogDebugWithRequestRetries, LogDebugWithRequestErrors")

	// sse is the server-side encryption algorithm used when storing this object in S3
	sse = flag.String("s3_backup_server_side_encryption", "", "server-side encryption algorithm (e.g., AES256, aws:kms, sse_c:/path/to/key/file)")

	// path component delimiter
	delimiter = "/"
)

type logNameToLogLevel map[string]aws.LogLevelType

var logNameMap logNameToLogLevel

const sseCustomerPrefix = "sse_c:"

// S3BackupHandle implements the backupstorage.BackupHandle interface.
type S3BackupHandle struct {
	client    s3iface.S3API
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

// RecordError is part of the concurrency.ErrorRecorder interface.
func (bh *S3BackupHandle) RecordError(err error) {
	bh.errors.RecordError(err)
}

// HasErrors is part of the concurrency.ErrorRecorder interface.
func (bh *S3BackupHandle) HasErrors() bool {
	return bh.errors.HasErrors()
}

// Error is part of the concurrency.ErrorRecorder interface.
func (bh *S3BackupHandle) Error() error {
	return bh.errors.Error()
}

// AddFile is part of the backupstorage.BackupHandle interface.
func (bh *S3BackupHandle) AddFile(ctx context.Context, filename string, filesize int64) (io.WriteCloser, error) {
	if bh.readOnly {
		return nil, fmt.Errorf("AddFile cannot be called on read-only backup")
	}

	// Calculate s3 upload part size using the source filesize
	partSizeBytes := s3manager.DefaultUploadPartSize
	if filesize > 0 {
		minimumPartSize := float64(filesize) / float64(s3manager.MaxUploadParts)
		// Round up to ensure large enough partsize
		calculatedPartSizeBytes := int64(math.Ceil(minimumPartSize))
		if calculatedPartSizeBytes > partSizeBytes {
			partSizeBytes = calculatedPartSizeBytes
		}
	}

	reader, writer := io.Pipe()
	bh.waitGroup.Add(1)

	go func() {
		defer bh.waitGroup.Done()
		uploader := s3manager.NewUploaderWithClient(bh.client, func(u *s3manager.Uploader) {
			u.PartSize = partSizeBytes
		})
		object := objName(bh.dir, bh.name, filename)

		_, err := uploader.Upload(&s3manager.UploadInput{
			Bucket:               bucket,
			Key:                  object,
			Body:                 reader,
			ServerSideEncryption: bh.bs.s3SSE.awsAlg,
			SSECustomerAlgorithm: bh.bs.s3SSE.customerAlg,
			SSECustomerKey:       bh.bs.s3SSE.customerKey,
			SSECustomerKeyMD5:    bh.bs.s3SSE.customerMd5,
		})
		if err != nil {
			reader.CloseWithError(err)
			bh.RecordError(err)
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
	return bh.Error()
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
		Bucket:               bucket,
		Key:                  object,
		SSECustomerAlgorithm: bh.bs.s3SSE.customerAlg,
		SSECustomerKey:       bh.bs.s3SSE.customerKey,
		SSECustomerKeyMD5:    bh.bs.s3SSE.customerMd5,
	})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

var _ backupstorage.BackupHandle = (*S3BackupHandle)(nil)

type S3ServerSideEncryption struct {
	awsAlg      *string
	customerAlg *string
	customerKey *string
	customerMd5 *string
}

func (s3ServerSideEncryption *S3ServerSideEncryption) init() error {
	s3ServerSideEncryption.reset()

	if strings.HasPrefix(*sse, sseCustomerPrefix) {
		sseCustomerKeyFile := strings.TrimPrefix(*sse, sseCustomerPrefix)
		base64CodedKey, err := ioutil.ReadFile(sseCustomerKeyFile)
		if err != nil {
			log.Errorf(err.Error())
			return err
		}

		decodedKey, err := base64.StdEncoding.DecodeString(string(base64CodedKey))
		if err != nil {
			decodedKey = base64CodedKey
		}

		md5Hash := md5.Sum(decodedKey)
		s3ServerSideEncryption.customerAlg = aws.String("AES256")
		s3ServerSideEncryption.customerKey = aws.String(string(decodedKey))
		s3ServerSideEncryption.customerMd5 = aws.String(base64.StdEncoding.EncodeToString(md5Hash[:]))
	} else if *sse != "" {
		s3ServerSideEncryption.awsAlg = sse
	}
	return nil
}

func (s3ServerSideEncryption *S3ServerSideEncryption) reset() {
	s3ServerSideEncryption.awsAlg = nil
	s3ServerSideEncryption.customerAlg = nil
	s3ServerSideEncryption.customerKey = nil
	s3ServerSideEncryption.customerMd5 = nil
}

// S3BackupStorage implements the backupstorage.BackupStorage interface.
type S3BackupStorage struct {
	_client *s3.S3
	mu      sync.Mutex
	s3SSE   S3ServerSideEncryption
}

// ListBackups is part of the backupstorage.BackupStorage interface.
func (bs *S3BackupStorage) ListBackups(ctx context.Context, dir string) ([]backupstorage.BackupHandle, error) {
	log.Infof("ListBackups: [s3] dir: %v, bucket: %v", dir, *bucket)
	c, err := bs.client()
	if err != nil {
		return nil, err
	}

	var searchPrefix *string
	if dir == "/" {
		searchPrefix = objName("")
	} else {
		searchPrefix = objName(dir, "")
	}
	log.Infof("objName: %v", searchPrefix)

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
	log.Infof("StartBackup: [s3] dir: %v, name: %v, bucket: %v", dir, name, *bucket)
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
	log.Infof("RemoveBackup: [s3] dir: %v, name: %v, bucket: %v", dir, name, *bucket)

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
	bs.s3SSE.reset()
	return nil
}

var _ backupstorage.BackupStorage = (*S3BackupStorage)(nil)

// getLogLevel converts the string loglevel to an aws.LogLevelType
func getLogLevel() *aws.LogLevelType {
	l := new(aws.LogLevelType)
	*l = aws.LogOff // default setting
	if level, found := logNameMap[*requiredLogLevel]; found {
		*l = level // adjust as required
	}
	return l
}

func (bs *S3BackupStorage) client() (*s3.S3, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	if bs._client == nil {
		logLevel := getLogLevel()

		tlsClientConf := &tls.Config{InsecureSkipVerify: *tlsSkipVerifyCert}
		httpTransport := &http.Transport{TLSClientConfig: tlsClientConf}
		httpClient := &http.Client{Transport: httpTransport}

		session, err := session.NewSession()
		if err != nil {
			return nil, err
		}

		awsConfig := aws.Config{
			HTTPClient:       httpClient,
			LogLevel:         logLevel,
			Endpoint:         aws.String(*endpoint),
			Region:           aws.String(*region),
			S3ForcePathStyle: aws.Bool(*forcePath),
		}

		if *retryCount >= 0 {
			awsConfig = *request.WithRetryer(&awsConfig, &ClosedConnectionRetryer{
				awsRetryer: &client.DefaultRetryer{
					NumMaxRetries: *retryCount,
				},
			})
		}

		bs._client = s3.New(session, &awsConfig)

		if len(*bucket) == 0 {
			return nil, fmt.Errorf("-s3_backup_storage_bucket required")
		}

		if _, err := bs._client.HeadBucket(&s3.HeadBucketInput{Bucket: bucket}); err != nil {
			return nil, err
		}

		if err := bs.s3SSE.init(); err != nil {
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

	logNameMap = logNameToLogLevel{
		"LogOff":                     aws.LogOff,
		"LogDebug":                   aws.LogDebug,
		"LogDebugWithSigning":        aws.LogDebugWithSigning,
		"LogDebugWithHTTPBody":       aws.LogDebugWithHTTPBody,
		"LogDebugWithRequestRetries": aws.LogDebugWithRequestRetries,
		"LogDebugWithRequestErrors":  aws.LogDebugWithRequestErrors,
	}
}

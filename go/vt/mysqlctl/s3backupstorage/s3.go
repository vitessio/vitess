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
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	transport "github.com/aws/smithy-go/endpoints"
	"github.com/aws/smithy-go/middleware"
	"github.com/dustin/go-humanize"
	"github.com/spf13/pflag"

	errorsbackup "vitess.io/vitess/go/vt/mysqlctl/errors"
	"vitess.io/vitess/go/vt/utils"

	"vitess.io/vitess/go/vt/log"
	stats "vitess.io/vitess/go/vt/mysqlctl/backupstats"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/servenv"
)

const (
	sseCustomerPrefix = "sse_c:"
	MaxPartSize       = 1024 * 1024 * 1024 * 5 // 5GiB - limited by AWS https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
)

var (
	// AWS API region
	region string

	// AWS request retries
	retryCount int

	// AWS endpoint, defaults to amazonaws.com but appliances may use a different location
	endpoint string

	// bucket is where the backups will go.
	bucket string

	// root is a prefix added to all object names.
	root string

	// forcePath is used to ensure that the certificate and path used match the endpoint + region
	forcePath bool

	tlsSkipVerifyCert bool

	// verboseLogging provides more verbose logging of AWS actions
	requiredLogLevel string

	// sse is the server-side encryption algorithm used when storing this object in S3
	sse string

	// path component delimiter
	delimiter = "/"

	// minimum part size
	minPartSize int64

	ErrPartSize = errors.New("minimum S3 part size must be between 5MiB and 5GiB")
)

func registerFlags(fs *pflag.FlagSet) {
	utils.SetFlagStringVar(fs, &region, "s3-backup-aws-region", "us-east-1", "AWS region to use.")
	utils.SetFlagIntVar(fs, &retryCount, "s3-backup-aws-retries", -1, "AWS request retries.")
	utils.SetFlagStringVar(fs, &endpoint, "s3-backup-aws-endpoint", "", "endpoint of the S3 backend (region must be provided).")
	utils.SetFlagStringVar(fs, &bucket, "s3-backup-storage-bucket", "", "S3 bucket to use for backups.")
	utils.SetFlagStringVar(fs, &root, "s3-backup-storage-root", "", "root prefix for all backup-related object names.")
	utils.SetFlagBoolVar(fs, &forcePath, "s3-backup-force-path-style", false, "force the s3 path style.")
	utils.SetFlagBoolVar(fs, &tlsSkipVerifyCert, "s3-backup-tls-skip-verify-cert", false, "skip the 'certificate is valid' check for SSL connections.")
	utils.SetFlagStringVar(fs, &requiredLogLevel, "s3-backup-log-level", "LogOff", "determine the S3 loglevel to use from LogOff, LogDebug, LogDebugWithSigning, LogDebugWithHTTPBody, LogDebugWithRequestRetries, LogDebugWithRequestErrors.")
	utils.SetFlagStringVar(fs, &sse, "s3-backup-server-side-encryption", "", "server-side encryption algorithm (e.g., AES256, aws:kms, sse_c:/path/to/key/file).")
	utils.SetFlagInt64Var(fs, &minPartSize, "s3-backup-aws-min-partsize", manager.MinUploadPartSize, "Minimum part size to use, defaults to 5MiB but can be increased due to the dataset size.")
}

func init() {
	servenv.OnParseFor("vtbackup", registerFlags)
	servenv.OnParseFor("vtctl", registerFlags)
	servenv.OnParseFor("vtctld", registerFlags)
	servenv.OnParseFor("vttablet", registerFlags)
}

type logNameToLogLevel map[string]aws.ClientLogMode

var logNameMap logNameToLogLevel

type endpointResolver struct {
	r        s3.EndpointResolverV2
	endpoint *string
}

func (er *endpointResolver) ResolveEndpoint(ctx context.Context, params s3.EndpointParameters) (transport.Endpoint, error) {
	params.Endpoint = er.endpoint
	return er.r.ResolveEndpoint(ctx, params)
}

func newEndpointResolver() *endpointResolver {
	return &endpointResolver{
		r:        s3.NewDefaultEndpointResolverV2(),
		endpoint: &endpoint,
	}
}

type iClient interface {
	manager.UploadAPIClient
	manager.DownloadAPIClient
}

type clientWrapper struct {
	*s3.Client
}

// S3BackupHandle implements the backupstorage.BackupHandle interface.
type S3BackupHandle struct {
	client    iClient
	bs        *S3BackupStorage
	dir       string
	name      string
	readOnly  bool
	waitGroup sync.WaitGroup
	errorsbackup.PerFileErrorRecorder
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
func (bh *S3BackupHandle) AddFile(ctx context.Context, filename string, filesize int64) (io.WriteCloser, error) {
	if bh.readOnly {
		return nil, errors.New("AddFile cannot be called on read-only backup")
	}

	partSizeBytes, err := calculateUploadPartSize(filesize)
	if err != nil {
		return nil, err
	}

	bh.bs.params.Logger.Infof("Using S3 upload part size: %s", humanize.IBytes(uint64(partSizeBytes)))

	reader, writer := io.Pipe()
	bh.handleAddFile(ctx, filename, partSizeBytes, reader, func(err error) {
		reader.CloseWithError(err)
	})

	return writer, nil
}

func (bh *S3BackupHandle) handleAddFile(ctx context.Context, filename string, partSizeBytes int64, reader io.Reader, closer func(error)) {
	bh.waitGroup.Add(1)

	go func() {
		defer bh.waitGroup.Done()
		uploader := manager.NewUploader(bh.client, func(u *manager.Uploader) {
			u.PartSize = partSizeBytes
		})
		object := objName(bh.dir, bh.name, filename)
		sendStats := bh.bs.params.Stats.Scope(stats.Operation("AWS:Request:Send"))
		_, err := uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket:               &bucket,
			Key:                  &object,
			Body:                 reader,
			ServerSideEncryption: bh.bs.s3SSE.awsAlg,
			SSECustomerAlgorithm: bh.bs.s3SSE.customerAlg,
			SSECustomerKey:       bh.bs.s3SSE.customerKey,
			SSECustomerKeyMD5:    bh.bs.s3SSE.customerMd5,
		}, func(u *manager.Uploader) {
			u.ClientOptions = append(u.ClientOptions, func(o *s3.Options) {
				o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
					return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc("CompleteAttemptMiddleware", func(ctx context.Context, input middleware.FinalizeInput, next middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
						start := time.Now()
						output, metadata, err := next.HandleFinalize(ctx, input)
						sendStats.TimedIncrement(time.Since(start))
						return output, metadata, err
					}), middleware.Before)
				})
			})
		})
		if err != nil {
			closer(err)
			bh.RecordError(filename, err)
		}
	}()
}

// calculateUploadPartSize is a helper to calculate the part size, taking into consideration the minimum part size
// passed in by an operator.
func calculateUploadPartSize(filesize int64) (partSizeBytes int64, err error) {
	// Calculate s3 upload part size using the source filesize
	partSizeBytes = manager.DefaultUploadPartSize
	if filesize > 0 {
		minimumPartSize := float64(filesize) / float64(manager.MaxUploadParts)
		// Round up to ensure large enough partsize
		calculatedPartSizeBytes := int64(math.Ceil(minimumPartSize))
		if calculatedPartSizeBytes > partSizeBytes {
			partSizeBytes = calculatedPartSizeBytes
		}
	}

	if minPartSize != 0 && partSizeBytes < minPartSize {
		if minPartSize > MaxPartSize || minPartSize < manager.MinUploadPartSize { // 5GiB and 5MiB respectively
			return 0, fmt.Errorf("%w, currently set to %s",
				ErrPartSize, humanize.IBytes(uint64(minPartSize)),
			)
		}
		partSizeBytes = int64(minPartSize)
	}

	return
}

// EndBackup is part of the backupstorage.BackupHandle interface.
func (bh *S3BackupHandle) EndBackup(ctx context.Context) error {
	if bh.readOnly {
		return errors.New("EndBackup cannot be called on read-only backup")
	}
	bh.waitGroup.Wait()
	return bh.Error()
}

// AbortBackup is part of the backupstorage.BackupHandle interface.
func (bh *S3BackupHandle) AbortBackup(ctx context.Context) error {
	if bh.readOnly {
		return errors.New("AbortBackup cannot be called on read-only backup")
	}
	return bh.bs.RemoveBackup(ctx, bh.dir, bh.name)
}

// ReadFile is part of the backupstorage.BackupHandle interface.
func (bh *S3BackupHandle) ReadFile(ctx context.Context, filename string) (io.ReadCloser, error) {
	if !bh.readOnly {
		return nil, errors.New("ReadFile cannot be called on read-write backup")
	}
	object := objName(bh.dir, bh.name, filename)
	sendStats := bh.bs.params.Stats.Scope(stats.Operation("AWS:Request:Send"))
	out, err := bh.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:               &bucket,
		Key:                  &object,
		SSECustomerAlgorithm: bh.bs.s3SSE.customerAlg,
		SSECustomerKey:       bh.bs.s3SSE.customerKey,
		SSECustomerKeyMD5:    bh.bs.s3SSE.customerMd5,
	}, func(o *s3.Options) {
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc("CompleteAttemptMiddleware", func(ctx context.Context, input middleware.FinalizeInput, next middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
				start := time.Now()
				output, metadata, err := next.HandleFinalize(ctx, input)
				sendStats.TimedIncrement(time.Since(start))
				return output, metadata, err
			}), middleware.Before)
		})
	})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

var _ backupstorage.BackupHandle = (*S3BackupHandle)(nil)

type S3ServerSideEncryption struct {
	awsAlg      types.ServerSideEncryption
	customerAlg *string
	customerKey *string
	customerMd5 *string
}

func (s3ServerSideEncryption *S3ServerSideEncryption) init() error {
	s3ServerSideEncryption.reset()

	if strings.HasPrefix(sse, sseCustomerPrefix) {
		sseCustomerKeyFile := strings.TrimPrefix(sse, sseCustomerPrefix)
		base64CodedKey, err := os.ReadFile(sseCustomerKeyFile)
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
	} else if sse != "" {
		s3ServerSideEncryption.awsAlg = types.ServerSideEncryption(sse)
	}
	return nil
}

func (s3ServerSideEncryption *S3ServerSideEncryption) reset() {
	s3ServerSideEncryption.awsAlg = ""
	s3ServerSideEncryption.customerAlg = nil
	s3ServerSideEncryption.customerKey = nil
	s3ServerSideEncryption.customerMd5 = nil
}

// S3BackupStorage implements the backupstorage.BackupStorage interface.
type S3BackupStorage struct {
	_client   *s3.Client
	mu        sync.Mutex
	s3SSE     S3ServerSideEncryption
	params    backupstorage.Params
	transport *http.Transport
}

func newS3BackupStorage() *S3BackupStorage {
	// This initialises a new transport based off http.DefaultTransport the first time and returns the same
	// transport on subsequent calls so connections can be reused as part of the same transport.
	tlsClientConf := &tls.Config{InsecureSkipVerify: tlsSkipVerifyCert}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsClientConf

	return &S3BackupStorage{params: backupstorage.NoParams(), transport: transport}
}

// ListBackups is part of the backupstorage.BackupStorage interface.
func (bs *S3BackupStorage) ListBackups(ctx context.Context, dir string) ([]backupstorage.BackupHandle, error) {
	log.Infof("ListBackups: [s3] dir: %v, bucket: %v", dir, bucket)
	c, err := bs.client()
	if err != nil {
		return nil, err
	}

	var searchPrefix string
	if dir == "/" {
		searchPrefix = objName("")
	} else {
		searchPrefix = objName(dir, "")
	}
	log.Infof("objName: %s", searchPrefix)

	query := &s3.ListObjectsV2Input{
		Bucket:    &bucket,
		Delimiter: &delimiter,
		Prefix:    &searchPrefix,
	}

	var subdirs []string
	for {
		objs, err := c.ListObjectsV2(ctx, query)
		if err != nil {
			return nil, err
		}
		for _, prefix := range objs.CommonPrefixes {
			subdir := strings.TrimPrefix(*prefix.Prefix, searchPrefix)
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
			client:   &clientWrapper{Client: c},
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
	log.Infof("StartBackup: [s3] dir: %v, name: %v, bucket: %v", dir, name, bucket)
	c, err := bs.client()
	if err != nil {
		return nil, err
	}

	return &S3BackupHandle{
		client:   &clientWrapper{Client: c},
		bs:       bs,
		dir:      dir,
		name:     name,
		readOnly: false,
	}, nil
}

// RemoveBackup is part of the backupstorage.BackupStorage interface.
func (bs *S3BackupStorage) RemoveBackup(ctx context.Context, dir, name string) error {
	log.Infof("RemoveBackup: [s3] dir: %v, name: %v, bucket: %v", dir, name, bucket)

	c, err := bs.client()
	if err != nil {
		return err
	}

	path := objName(dir, name)
	query := &s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &path,
	}

	for {
		objs, err := c.ListObjectsV2(ctx, query)
		if err != nil {
			return err
		}

		objIds := make([]types.ObjectIdentifier, 0, len(objs.Contents))
		for _, obj := range objs.Contents {
			objIds = append(objIds, types.ObjectIdentifier{
				Key: obj.Key,
			})
		}

		quiet := true // return less in the Delete response
		out, err := c.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: &bucket,
			Delete: &types.Delete{
				Objects: objIds,
				Quiet:   &quiet,
			},
		})

		if err != nil {
			return err
		}

		for _, objError := range out.Errors {
			return errors.New(*objError.Message)
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

func (bs *S3BackupStorage) WithParams(params backupstorage.Params) backupstorage.BackupStorage {
	return &S3BackupStorage{params: params, transport: bs.transport}
}

var _ backupstorage.BackupStorage = (*S3BackupStorage)(nil)

// getLogLevel converts the string loglevel to an aws.LogLevelType
func getLogLevel() aws.ClientLogMode {
	var l aws.ClientLogMode
	if level, found := logNameMap[requiredLogLevel]; found {
		l = level // adjust as required
	}
	return l
}

func (bs *S3BackupStorage) client() (*s3.Client, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	if bs._client == nil {
		logLevel := getLogLevel()

		httpClient := &http.Client{Transport: bs.transport}

		cfg, err := config.LoadDefaultConfig(context.Background(),
			config.WithRegion(region),
			config.WithClientLogMode(logLevel),
			config.WithHTTPClient(httpClient),
		)
		if err != nil {
			return nil, err
		}

		options := []func(options *s3.Options){
			func(o *s3.Options) {
				o.UsePathStyle = forcePath
				if retryCount >= 0 {
					o.RetryMaxAttempts = retryCount
					o.Retryer = &ClosedConnectionRetryer{
						awsRetryer: retry.NewStandard(func(options *retry.StandardOptions) {
							options.MaxAttempts = retryCount
						}),
					}
				}
			},
		}
		if endpoint != "" {
			options = append(options, s3.WithEndpointResolverV2(newEndpointResolver()))
		}

		bs._client = s3.NewFromConfig(cfg, options...)

		if len(bucket) == 0 {
			return nil, errors.New("--s3-backup-storage-bucket required")
		}

		if _, err := bs._client.HeadBucket(context.Background(), &s3.HeadBucketInput{Bucket: &bucket}); err != nil {
			return nil, err
		}

		if err := bs.s3SSE.init(); err != nil {
			return nil, err
		}
	}
	return bs._client, nil
}

func objName(parts ...string) string {
	res := ""
	if root != "" {
		res += root + delimiter
	}
	res += strings.Join(parts, delimiter)
	return res
}

func init() {
	backupstorage.BackupStorageMap["s3"] = newS3BackupStorage()

	logNameMap = logNameToLogLevel{
		"LogOff":                     0,
		"LogDebug":                   aws.LogRequest,
		"LogDebugWithSigning":        aws.LogSigning,
		"LogDebugWithHTTPBody":       aws.LogRequestWithBody,
		"LogDebugWithRequestRetries": aws.LogRetries,
		"LogDebugWithRequestErrors":  aws.LogRequest | aws.LogRetries,
	}
}

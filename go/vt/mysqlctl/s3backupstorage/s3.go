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
	"bufio"
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
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	tmtypes "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	transport "github.com/aws/smithy-go/endpoints"
	"github.com/aws/smithy-go/middleware"
	"github.com/dustin/go-humanize"
	"github.com/spf13/pflag"

	errorsbackup "vitess.io/vitess/go/vt/mysqlctl/errors"

	"vitess.io/vitess/go/vt/log"
	stats "vitess.io/vitess/go/vt/mysqlctl/backupstats"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/servenv"
)

const (
	sseCustomerPrefix = "sse_c:"
	MaxPartSize       = 1024 * 1024 * 1024 * 5 // 5GiB - limited by AWS https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html

	// maxPerFileMemory caps the transfer-manager buffer per concurrent restore
	// file. Restore opens up to 4 files concurrently, so this limits total
	// download buffering to ~4 GiB in the worst case.
	maxPerFileMemory int64 = 1024 * 1024 * 1024 // 1 GiB
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

	// minimum download part size — separate from upload so the two can be configured independently
	minDownloadPartSize int64 = 5 * 1024 * 1024 // 5MiB - S3 minimum for byte-range requests

	// download part size and concurrency for parallel downloads via transfer manager
	downloadPartSize    int64 = 8 * 1024 * 1024 // 8MiB - transfer manager default
	downloadConcurrency int   = 1               // default preserves old single-stream GetObject path

	ErrPartSize = errors.New("minimum S3 part size must be between 5MiB and 5GiB")
)

func registerFlags(fs *pflag.FlagSet) {
	fs.StringVar(&region, "s3_backup_aws_region", "us-east-1", "AWS region to use.")
	fs.IntVar(&retryCount, "s3_backup_aws_retries", -1, "AWS request retries.")
	fs.StringVar(&endpoint, "s3_backup_aws_endpoint", "", "endpoint of the S3 backend (region must be provided).")
	fs.StringVar(&bucket, "s3_backup_storage_bucket", "", "S3 bucket to use for backups.")
	fs.StringVar(&root, "s3_backup_storage_root", "", "root prefix for all backup-related object names.")
	fs.BoolVar(&forcePath, "s3_backup_force_path_style", false, "force the s3 path style.")
	fs.BoolVar(&tlsSkipVerifyCert, "s3_backup_tls_skip_verify_cert", false, "skip the 'certificate is valid' check for SSL connections.")
	fs.StringVar(&requiredLogLevel, "s3_backup_log_level", "LogOff", "determine the S3 loglevel to use from LogOff, LogDebug, LogDebugWithSigning, LogDebugWithHTTPBody, LogDebugWithRequestRetries, LogDebugWithRequestErrors.")
	fs.StringVar(&sse, "s3_backup_server_side_encryption", "", "server-side encryption algorithm (e.g., AES256, aws:kms, sse_c:/path/to/key/file).")
	fs.Int64Var(&minPartSize, "s3_backup_aws_min_partsize", manager.MinUploadPartSize, "Minimum part size to use, defaults to 5MiB but can be increased due to the dataset size.")
	fs.Int64Var(&downloadPartSize, "s3-backup-download-part-size", 8*1024*1024, "Part size in bytes for parallel S3 downloads via transfer manager.")
	fs.IntVar(&downloadConcurrency, "s3-backup-download-concurrency", 1, "Number of parallel goroutines for S3 downloads. Set > 1 to enable parallel byte-range GETs via the transfer manager (recommended: 2-10).")
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

// S3BackupHandle implements the backupstorage.BackupHandle interface.
type S3BackupHandle struct {
	s3Client *s3.Client
	bs       *S3BackupStorage
	dir      string
	name     string
	readOnly bool
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
		return nil, fmt.Errorf("AddFile cannot be called on read-only backup")
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
		uploader := manager.NewUploader(bh.s3Client, func(u *manager.Uploader) {
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

// Wait is part of the backupstorage.BackupHandle interface.
func (bh *S3BackupHandle) Wait() {
	bh.waitGroup.Wait()
}

// EndBackup is part of the backupstorage.BackupHandle interface.
func (bh *S3BackupHandle) EndBackup(ctx context.Context) error {
	if bh.readOnly {
		return fmt.Errorf("EndBackup cannot be called on read-only backup")
	}
	bh.Wait()
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
	sendStats := bh.bs.params.Stats.Scope(stats.Operation("AWS:Request:Send"))

	withTiming := func(o *s3.Options) {
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc("CompleteAttemptMiddleware", func(ctx context.Context, input middleware.FinalizeInput, next middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
				start := time.Now()
				output, metadata, err := next.HandleFinalize(ctx, input)
				sendStats.TimedIncrement(time.Since(start))
				return output, metadata, err
			}), middleware.Before)
		})
	}

	// When concurrency <= 1 (the default), use a plain GetObject — no HeadObject,
	// no ranged GETs, no transfer manager overhead. Users opt into parallel
	// downloads by setting --s3-backup-download-concurrency > 1.
	if downloadConcurrency <= 1 {
		out, err := bh.s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket:               &bucket,
			Key:                  &object,
			SSECustomerAlgorithm: bh.bs.s3SSE.customerAlg,
			SSECustomerKey:       bh.bs.s3SSE.customerKey,
			SSECustomerKeyMD5:    bh.bs.s3SSE.customerMd5,
		}, withTiming)
		if err != nil {
			return nil, err
		}
		return out.Body, nil
	}

	if downloadPartSize < minDownloadPartSize {
		return nil, fmt.Errorf("--s3-backup-download-part-size must be >= %d (5 MiB), got %d", minDownloadPartSize, downloadPartSize)
	}

	bufferSize, err := downloadBufferSize(downloadPartSize, downloadConcurrency)
	if err != nil {
		return nil, err
	}

	// The transfer manager calls HeadObject internally to determine object size,
	// but does not forward SSE-C params to that call (aws-sdk-go-v2 bug). We work
	// around this by wrapping the client to inject SSE-C params into HeadObject.
	var tmS3Client transfermanager.S3APIClient = bh.s3Client
	if bh.bs.s3SSE.customerAlg != nil {
		tmS3Client = &sseCClient{
			S3APIClient: bh.s3Client,
			alg:         bh.bs.s3SSE.customerAlg,
			key:         bh.bs.s3SSE.customerKey,
			keyMD5:      bh.bs.s3SSE.customerMd5,
		}
	}

	tmClient := transfermanager.New(tmS3Client, func(o *transfermanager.Options) {
		// GetObjectRanges uses byte-range GETs sized by PartSizeBytes.
		// The default (GetObjectParts) reuses original multipart part numbers
		// and ignores PartSizeBytes entirely.
		o.GetObjectType = tmtypes.GetObjectRanges
		o.PartSizeBytes = downloadPartSize
		o.Concurrency = downloadConcurrency
		o.GetObjectBufferSize = bufferSize
	})

	readCtx, cancel := context.WithCancel(ctx)
	out, err := tmClient.GetObject(readCtx, &transfermanager.GetObjectInput{
		Bucket:               &bucket,
		Key:                  &object,
		SSECustomerAlgorithm: bh.bs.s3SSE.customerAlg,
		SSECustomerKey:       bh.bs.s3SSE.customerKey,
		SSECustomerKeyMD5:    bh.bs.s3SSE.customerMd5,
	})
	if err != nil {
		cancel()
		return nil, err
	}

	// The transfer manager's concurrentReader spawns Concurrency goroutines per
	// Read() call. Vitess's restore pipe (pgzip) reads through a small buffer
	// (~4 KiB), so without coalescing, each tiny Read() creates a full worker
	// pool — ~1.3M goroutine lifecycles per GiB. A one-part bufio.Reader is
	// sufficient: the SDK's GetObjectBufferSize already manages the full transfer
	// window internally; this buffer only needs to coalesce small downstream reads
	// into a part-sized read for the SDK.
	body := io.Reader(out.Body)
	if testBodyWrapHook != nil {
		body = testBodyWrapHook(body)
	}
	buffered := bufio.NewReaderSize(body, int(downloadPartSize))

	return &cancelingReader{Reader: buffered, body: out.Body, cancel: cancel}, nil
}

// testBodyWrapHook allows tests to wrap the SDK body reader before it's passed
// to bufio.NewReaderSize, enabling read-counting without pinning the
// coalescing implementation.
var testBodyWrapHook func(io.Reader) io.Reader

// downloadBufferSize computes the GetObjectBufferSize and validates that the
// resulting per-file memory usage stays within maxPerFileMemory. The total
// per-file allocation is the SDK's GetObjectBufferSize (partSize × concurrency)
// plus one part for the bufio.Reader that coalesces small reads.
func downloadBufferSize(partSize int64, concurrency int) (int64, error) {
	if partSize > math.MaxInt64/int64(concurrency) {
		return 0, fmt.Errorf("--s3-backup-download-part-size (%d) * --s3-backup-download-concurrency (%d) overflows int64", partSize, concurrency)
	}
	size := partSize * int64(concurrency)
	if size > math.MaxInt64-partSize {
		return 0, fmt.Errorf("--s3-backup-download-part-size (%d) * --s3-backup-download-concurrency (%d) + part size overflows int64", partSize, concurrency)
	}
	totalPerFile := size + partSize
	if totalPerFile > maxPerFileMemory {
		return 0, fmt.Errorf(
			"per-file memory (SDK buffer %s + read buffer %s = %s) exceeds limit of %s; reduce --s3-backup-download-part-size or --s3-backup-download-concurrency",
			humanize.IBytes(uint64(size)), humanize.IBytes(uint64(partSize)),
			humanize.IBytes(uint64(totalPerFile)), humanize.IBytes(uint64(maxPerFileMemory)),
		)
	}
	return size, nil
}

// cancelingReader wraps a transfer-manager reader with context cancellation and
// proper resource cleanup. It also preserves the underlying body's Close method
// for the zero-length object path where the SDK returns the raw S3 response body.
type cancelingReader struct {
	io.Reader
	body   io.Reader
	cancel context.CancelFunc
}

func (r *cancelingReader) Close() error {
	r.cancel()
	if c, ok := r.body.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// sseCClient wraps an S3APIClient to inject SSE-C encryption params into
// HeadObject calls. This works around a transfer manager bug where its
// internal HeadObject (used to discover object size) omits SSE-C fields,
// causing 403 errors for customer-encrypted objects.
type sseCClient struct {
	transfermanager.S3APIClient
	alg    *string
	key    *string
	keyMD5 *string
}

func (c *sseCClient) HeadObject(ctx context.Context, input *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	input.SSECustomerAlgorithm = c.alg
	input.SSECustomerKey = c.key
	input.SSECustomerKeyMD5 = c.keyMD5
	return c.S3APIClient.HeadObject(ctx, input, optFns...)
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
			s3Client: c,
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
		s3Client: c,
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

		cfg, err := config.LoadDefaultConfig(
			context.Background(),
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
			return nil, fmt.Errorf("--s3_backup_storage_bucket required")
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

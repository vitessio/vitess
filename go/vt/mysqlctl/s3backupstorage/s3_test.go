package s3backupstorage

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/logutil"
	stats "vitess.io/vitess/go/vt/mysqlctl/backupstats"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
)

// mockS3Server provides a mock S3 HTTP server for testing
type mockS3Server struct {
	server        *httptest.Server
	requestCount  int
	requestDelay  time.Duration
	shouldError   bool
	errorAfter    int
	uploadedParts map[string][][]byte
	mu            sync.Mutex
}

func newMockS3Server() *mockS3Server {
	m := &mockS3Server{
		uploadedParts: make(map[string][][]byte),
	}

	m.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.requestCount++
		reqCount := m.requestCount
		delay := m.requestDelay
		shouldError := m.shouldError
		errorAfter := m.errorAfter
		m.mu.Unlock()

		if delay > 0 {
			time.Sleep(delay)
		}

		if shouldError && (errorAfter == 0 || reqCount > errorAfter) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<Error>
	<Code>InternalError</Code>
	<Message>Internal Server Error</Message>
</Error>`))
			return
		}

		// Handle different S3 operations
		if r.Method == "POST" && strings.Contains(r.URL.RawQuery, "uploads") {
			// InitiateMultipartUpload
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult>
	<Bucket>test-bucket</Bucket>
	<Key>test-key</Key>
	<UploadId>test-upload-id</UploadId>
</InitiateMultipartUploadResult>`))
		} else if r.Method == "PUT" && strings.Contains(r.URL.RawQuery, "partNumber") {
			// UploadPart
			body, _ := io.ReadAll(r.Body)
			m.mu.Lock()
			m.uploadedParts[r.URL.Path] = append(m.uploadedParts[r.URL.Path], body)
			m.mu.Unlock()

			w.Header().Set("ETag", `"test-etag"`)
			w.WriteHeader(http.StatusOK)
		} else if r.Method == "POST" && strings.Contains(r.URL.RawQuery, "uploadId") {
			// CompleteMultipartUpload
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult>
	<Location>https://test-bucket.s3.amazonaws.com/test-key</Location>
	<Bucket>test-bucket</Bucket>
	<Key>test-key</Key>
	<ETag>"test-etag"</ETag>
</CompleteMultipartUploadResult>`))
		} else if r.Method == "PUT" {
			// PutObject (single upload)
			body, _ := io.ReadAll(r.Body)
			m.mu.Lock()
			m.uploadedParts[r.URL.Path] = [][]byte{body}
			m.mu.Unlock()

			w.Header().Set("ETag", `"test-etag"`)
			w.WriteHeader(http.StatusOK)
		} else if r.Method == "GET" && !strings.Contains(r.URL.RawQuery, "list-type") {
			// GetObject
			m.mu.Lock()
			parts := m.uploadedParts[r.URL.Path]
			m.mu.Unlock()

			if len(parts) > 0 {
				w.WriteHeader(http.StatusOK)
				for _, part := range parts {
					w.Write(part)
				}
			} else {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<Error>
	<Code>NoSuchKey</Code>
	<Message>The specified key does not exist.</Message>
</Error>`))
			}
		} else if r.Method == "GET" && strings.Contains(r.URL.RawQuery, "list-type=2") {
			// ListObjectsV2
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListObjectsV2Response>
	<IsTruncated>false</IsTruncated>
	<Contents>
		<Key>test-key</Key>
		<ETag>"test-etag"</ETag>
	</Contents>
</ListObjectsV2Response>`))
		} else if r.Method == "POST" && strings.Contains(r.URL.RawQuery, "delete") {
			// DeleteObjects
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult>
</DeleteResult>`))
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))

	return m
}

func (m *mockS3Server) Close() {
	m.server.Close()
}

func (m *mockS3Server) URL() string {
	return m.server.URL
}

func (m *mockS3Server) RequestCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requestCount
}

func (m *mockS3Server) SetDelay(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requestDelay = d
}

func (m *mockS3Server) SetError(shouldError bool, errorAfter int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shouldError = shouldError
	m.errorAfter = errorAfter
}

func createTestS3Client(mockServer *mockS3Server) *s3.Client {
	return s3.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: aws.AnonymousCredentials{},
	}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(mockServer.URL())
		o.UsePathStyle = true
	})
}

func TestAddFileError(t *testing.T) {
	mockServer := newMockS3Server()
	defer mockServer.Close()

	mockServer.SetError(true, 0)

	bh := &S3BackupHandle{
		s3Client: createTestS3Client(mockServer),
		bs: &S3BackupStorage{
			params: backupstorage.NoParams(),
			s3SSE: S3ServerSideEncryption{
				customerAlg: new(string),
				customerKey: new(string),
				customerMd5: new(string),
			},
		},
		readOnly: false,
	}

	wc, err := bh.AddFile(context.Background(), "somefile", 100000)
	require.NoError(t, err, "AddFile() should not error on creation")
	assert.NotNil(t, wc, "AddFile() expected non-nil WriteCloser")

	n, err := wc.Write([]byte("here are some bytes"))
	require.NoError(t, err, "Write() should not error")
	require.Equal(t, 19, n)

	err = wc.Close()
	require.NoError(t, err, "Close() should not error")

	bh.waitGroup.Wait()

	require.True(t, bh.HasErrors(), "AddFile() expected bh to record async error but did not")
}

func TestAddFileStats(t *testing.T) {
	mockServer := newMockS3Server()
	defer mockServer.Close()

	fakeStats := stats.NewFakeStats()

	bh := &S3BackupHandle{
		s3Client: createTestS3Client(mockServer),
		bs: &S3BackupStorage{
			params: backupstorage.Params{
				Logger: logutil.NewMemoryLogger(),
				Stats:  fakeStats,
			},
			s3SSE: S3ServerSideEncryption{
				customerAlg: new(string),
				customerKey: new(string),
				customerMd5: new(string),
			},
		},
		readOnly: false,
	}

	for i := range 4 {
		wc, err := bh.AddFile(context.Background(), fmt.Sprintf("somefile-%d", i), 100000)
		require.NoError(t, err, "AddFile() expected no error")
		assert.NotNil(t, wc, "AddFile() expected non-nil WriteCloser")

		n, err := wc.Write([]byte("here are some bytes"))
		require.NoError(t, err, "Write() should not error")
		require.Equal(t, 19, n)

		err = wc.Close()
		require.NoError(t, err, "Close() should not error")
	}

	bh.waitGroup.Wait()

	require.False(t, bh.HasErrors(), "AddFile() should not have recorded errors")

	// Verify that stats were collected for each upload
	require.Len(t, fakeStats.ScopeCalls, 4)
	scopedStats := fakeStats.ScopeReturns[0]
	require.Len(t, scopedStats.ScopeV, 1)
	require.Equal(t, scopedStats.ScopeV[stats.ScopeOperation], "AWS:Request:Send")
	require.Len(t, scopedStats.TimedIncrementCalls, 1)
	require.Len(t, scopedStats.TimedIncrementBytesCalls, 0)
}

func TestAddFileErrorStats(t *testing.T) {
	mockServer := newMockS3Server()
	defer mockServer.Close()

	mockServer.SetError(true, 0)

	fakeStats := stats.NewFakeStats()

	bh := &S3BackupHandle{
		s3Client: createTestS3Client(mockServer),
		bs: &S3BackupStorage{
			params: backupstorage.Params{
				Logger: logutil.NewMemoryLogger(),
				Stats:  fakeStats,
			},
			s3SSE: S3ServerSideEncryption{
				customerAlg: new(string),
				customerKey: new(string),
				customerMd5: new(string),
			},
		},
		readOnly: false,
	}

	wc, err := bh.AddFile(context.Background(), "somefile", 100000)
	require.NoError(t, err, "AddFile() should not error on creation")
	assert.NotNil(t, wc, "AddFile() expected non-nil WriteCloser")

	n, err := wc.Write([]byte("here are some bytes"))
	require.NoError(t, err, "Write() should not error")
	require.Equal(t, 19, n)

	err = wc.Close()
	require.NoError(t, err, "Close() should not error")

	bh.waitGroup.Wait()

	require.True(t, bh.HasErrors(), "AddFile() expected bh to record async error")

	// Stats should still be collected even when there's an error
	require.Len(t, fakeStats.ScopeCalls, 1)
	scopedStats := fakeStats.ScopeReturns[0]
	require.Len(t, scopedStats.ScopeV, 1)
	require.Equal(t, scopedStats.ScopeV[stats.ScopeOperation], "AWS:Request:Send")
	require.Len(t, scopedStats.TimedIncrementCalls, 1)
	require.Len(t, scopedStats.TimedIncrementBytesCalls, 0)
}

func TestReadFile(t *testing.T) {
	mockServer := newMockS3Server()
	defer mockServer.Close()

	originalBucket := bucket
	originalRoot := root
	defer func() {
		bucket = originalBucket
		root = originalRoot
	}()

	bucket = "test-bucket"
	root = ""

	testData := []byte("test file contents")

	// First upload the data
	bh := &S3BackupHandle{
		s3Client: createTestS3Client(mockServer),
		bs: &S3BackupStorage{
			params: backupstorage.NoParams(),
			s3SSE: S3ServerSideEncryption{
				customerAlg: new(string),
				customerKey: new(string),
				customerMd5: new(string),
			},
		},
		dir:      "testdir",
		name:     "testbackup",
		readOnly: false,
	}

	wc, err := bh.AddFile(context.Background(), "testfile", 100)
	require.NoError(t, err)
	_, err = wc.Write(testData)
	require.NoError(t, err)
	err = wc.Close()
	require.NoError(t, err)
	bh.waitGroup.Wait()

	// Now read it back
	readBh := &S3BackupHandle{
		s3Client: createTestS3Client(mockServer),
		bs: &S3BackupStorage{
			params: backupstorage.NoParams(),
			s3SSE: S3ServerSideEncryption{
				customerAlg: new(string),
				customerKey: new(string),
				customerMd5: new(string),
			},
		},
		dir:      "testdir",
		name:     "testbackup",
		readOnly: true,
	}

	rc, err := readBh.ReadFile(context.Background(), "testfile")
	require.NoError(t, err, "ReadFile() should not error")
	require.NotNil(t, rc, "ReadFile() should return non-nil ReadCloser")

	data, err := io.ReadAll(rc)
	require.NoError(t, err, "ReadAll() should not error")
	require.Equal(t, testData, data, "Read data should match uploaded data")

	err = rc.Close()
	require.NoError(t, err, "Close() should not error")
}

func TestReadFileOnWriteHandle(t *testing.T) {
	bh := &S3BackupHandle{
		readOnly: false,
	}

	_, err := bh.ReadFile(context.Background(), "testfile")
	require.Error(t, err, "ReadFile() should error on write handle")
	require.Contains(t, err.Error(), "cannot be called on read-write backup")
}

func TestAddFileOnReadOnlyHandle(t *testing.T) {
	bh := &S3BackupHandle{
		readOnly: true,
	}

	_, err := bh.AddFile(context.Background(), "testfile", 100)
	require.Error(t, err, "AddFile() should error on read-only handle")
	require.Contains(t, err.Error(), "cannot be called on read-only backup")
}

func TestEndBackupOnReadOnlyHandle(t *testing.T) {
	bh := &S3BackupHandle{
		readOnly: true,
	}

	err := bh.EndBackup(context.Background())
	require.Error(t, err, "EndBackup() should error on read-only handle")
	require.Contains(t, err.Error(), "cannot be called on read-only backup")
}

func TestAbortBackupOnReadOnlyHandle(t *testing.T) {
	bh := &S3BackupHandle{
		readOnly: true,
	}

	err := bh.AbortBackup(context.Background())
	require.Error(t, err, "AbortBackup() should error on read-only handle")
	require.Contains(t, err.Error(), "cannot be called on read-only backup")
}

func TestBackupHandleGetters(t *testing.T) {
	bh := &S3BackupHandle{
		dir:  "test-dir",
		name: "test-name",
	}

	require.Equal(t, "test-dir", bh.Directory())
	require.Equal(t, "test-name", bh.Name())
}

func TestEndBackup(t *testing.T) {
	mockServer := newMockS3Server()
	defer mockServer.Close()

	bh := &S3BackupHandle{
		s3Client: createTestS3Client(mockServer),
		bs: &S3BackupStorage{
			params: backupstorage.NoParams(),
			s3SSE: S3ServerSideEncryption{
				customerAlg: new(string),
				customerKey: new(string),
				customerMd5: new(string),
			},
		},
		readOnly: false,
	}

	// Add a file
	wc, err := bh.AddFile(context.Background(), "testfile", 100)
	require.NoError(t, err)
	wc.Write([]byte("test data"))
	wc.Close()

	// End the backup
	err = bh.EndBackup(context.Background())
	require.NoError(t, err, "EndBackup() should not error")
	require.False(t, bh.HasErrors(), "EndBackup() should not have errors")
}

func TestEndBackupWithError(t *testing.T) {
	mockServer := newMockS3Server()
	defer mockServer.Close()

	mockServer.SetError(true, 0)

	bh := &S3BackupHandle{
		s3Client: createTestS3Client(mockServer),
		bs: &S3BackupStorage{
			params: backupstorage.NoParams(),
			s3SSE: S3ServerSideEncryption{
				customerAlg: new(string),
				customerKey: new(string),
				customerMd5: new(string),
			},
		},
		readOnly: false,
	}

	// Add a file that will fail to upload
	wc, err := bh.AddFile(context.Background(), "testfile", 100)
	require.NoError(t, err)
	wc.Write([]byte("test data"))
	wc.Close()

	// End the backup - should return the error
	err = bh.EndBackup(context.Background())
	require.Error(t, err, "EndBackup() should return error when upload fails")
}

func TestCalculateUploadPartSizeEdgeCases(t *testing.T) {
	originalMinimum := minPartSize
	defer func() { minPartSize = originalMinimum }()

	// Test with zero filesize
	minPartSize = 0
	partSize, err := calculateUploadPartSize(0)
	require.NoError(t, err)
	require.Equal(t, int64(5*1024*1024), partSize) // Should be default

	// Test with negative filesize
	partSize, err = calculateUploadPartSize(-100)
	require.NoError(t, err)
	require.Equal(t, int64(5*1024*1024), partSize)
}

func TestNoSSE(t *testing.T) {
	sseData := S3ServerSideEncryption{}
	err := sseData.init()
	require.NoError(t, err, "init() expected to succeed")

	assert.Empty(t, sseData.awsAlg, "awsAlg expected to be empty")
	assert.Nil(t, sseData.customerAlg, "customerAlg expected to be nil")
	assert.Nil(t, sseData.customerKey, "customerKey expected to be nil")
	assert.Nil(t, sseData.customerMd5, "customerMd5 expected to be nil")

	sseData.reset()
	require.NoError(t, err, "reset() expected to succeed")
}

func TestSSEAws(t *testing.T) {
	sse = "aws:kms"
	sseData := S3ServerSideEncryption{}
	err := sseData.init()
	require.NoError(t, err, "init() expected to succeed")

	assert.Equal(t, types.ServerSideEncryption("aws:kms"), sseData.awsAlg, "awsAlg expected to be aws:kms")
	assert.Nil(t, sseData.customerAlg, "customerAlg expected to be nil")
	assert.Nil(t, sseData.customerKey, "customerKey expected to be nil")
	assert.Nil(t, sseData.customerMd5, "customerMd5 expected to be nil")

	sseData.reset()
	require.NoError(t, err, "reset() expected to succeed")

	assert.Empty(t, sseData.awsAlg, "awsAlg expected to be empty")
	assert.Nil(t, sseData.customerAlg, "customerAlg expected to be nil")
	assert.Nil(t, sseData.customerKey, "customerKey expected to be nil")
	assert.Nil(t, sseData.customerMd5, "customerMd5 expected to be nil")
}

func TestSSECustomerFileNotFound(t *testing.T) {
	tempFile, err := os.CreateTemp("", "filename")
	require.NoError(t, err, "TempFile() expected to succeed")
	defer os.Remove(tempFile.Name())

	err = tempFile.Close()
	require.NoError(t, err, "Close() expected to succeed")

	err = os.Remove(tempFile.Name())
	require.NoError(t, err, "Remove() expected to succeed")

	sse = sseCustomerPrefix + tempFile.Name()
	sseData := S3ServerSideEncryption{}
	err = sseData.init()
	require.Error(t, err, "init() expected to fail")
}

func TestSSECustomerFileBinaryKey(t *testing.T) {
	tempFile, err := os.CreateTemp("", "filename")
	require.NoError(t, err, "TempFile() expected to succeed")
	defer os.Remove(tempFile.Name())

	randomKey := make([]byte, 32)
	_, err = rand.Read(randomKey)
	require.NoError(t, err, "Read() expected to succeed")
	_, err = tempFile.Write(randomKey)
	require.NoError(t, err, "Write() expected to succeed")
	err = tempFile.Close()
	require.NoError(t, err, "Close() expected to succeed")

	sse = sseCustomerPrefix + tempFile.Name()
	sseData := S3ServerSideEncryption{}
	err = sseData.init()
	require.NoError(t, err, "init() expected to succeed")

	assert.Empty(t, sseData.awsAlg, "awsAlg expected to be empty")
	assert.Equal(t, aws.String("AES256"), sseData.customerAlg, "customerAlg expected to be AES256")
	assert.Equal(t, aws.String(string(randomKey)), sseData.customerKey, "customerKey expected to be equal to the generated randomKey")
	md5Hash := md5.Sum(randomKey)
	assert.Equal(t, aws.String(base64.StdEncoding.EncodeToString(md5Hash[:])), sseData.customerMd5, "customerMd5 expected to be equal to the customerMd5 hash of the generated randomKey")

	sseData.reset()
	require.NoError(t, err, "reset() expected to succeed")

	assert.Empty(t, sseData.awsAlg, "awsAlg expected to be empty")
	assert.Nil(t, sseData.customerAlg, "customerAlg expected to be nil")
	assert.Nil(t, sseData.customerKey, "customerKey expected to be nil")
	assert.Nil(t, sseData.customerMd5, "customerMd5 expected to be nil")
}

func TestSSECustomerFileBase64Key(t *testing.T) {
	tempFile, err := os.CreateTemp("", "filename")
	require.NoError(t, err, "TempFile() expected to succeed")
	defer os.Remove(tempFile.Name())

	randomKey := make([]byte, 32)
	_, err = rand.Read(randomKey)
	require.NoError(t, err, "Read() expected to succeed")

	base64Key := base64.StdEncoding.EncodeToString(randomKey[:])
	_, err = tempFile.WriteString(base64Key)
	require.NoError(t, err, "WriteString() expected to succeed")
	err = tempFile.Close()
	require.NoError(t, err, "Close() expected to succeed")

	sse = sseCustomerPrefix + tempFile.Name()
	sseData := S3ServerSideEncryption{}
	err = sseData.init()
	require.NoError(t, err, "init() expected to succeed")

	assert.Empty(t, sseData.awsAlg, "awsAlg expected to be empty")
	assert.Equal(t, aws.String("AES256"), sseData.customerAlg, "customerAlg expected to be AES256")
	assert.Equal(t, aws.String(string(randomKey)), sseData.customerKey, "customerKey expected to be equal to the generated randomKey")
	md5Hash := md5.Sum(randomKey)
	assert.Equal(t, aws.String(base64.StdEncoding.EncodeToString(md5Hash[:])), sseData.customerMd5, "customerMd5 expected to be equal to the customerMd5 hash of the generated randomKey")

	sseData.reset()
	require.NoError(t, err, "reset() expected to succeed")

	assert.Empty(t, sseData.awsAlg, "awsAlg expected to be empty")
	assert.Nil(t, sseData.customerAlg, "customerAlg expected to be nil")
	assert.Nil(t, sseData.customerKey, "customerKey expected to be nil")
	assert.Nil(t, sseData.customerMd5, "customerMd5 expected to be nil")
}

func TestNewS3Transport(t *testing.T) {
	s3 := newS3BackupStorage()

	// checking some of the values are present in the returned transport and match the http.DefaultTransport.
	assert.Equal(t, http.DefaultTransport.(*http.Transport).IdleConnTimeout, s3.transport.IdleConnTimeout)
	assert.Equal(t, http.DefaultTransport.(*http.Transport).MaxIdleConns, s3.transport.MaxIdleConns)
	assert.NotNil(t, s3.transport.DialContext)
	assert.NotNil(t, s3.transport.Proxy)
}

func TestWithParams(t *testing.T) {
	bases3 := newS3BackupStorage()
	s3 := bases3.WithParams(backupstorage.Params{}).(*S3BackupStorage)
	// checking some of the values are present in the returned transport and match the http.DefaultTransport.
	assert.Equal(t, http.DefaultTransport.(*http.Transport).IdleConnTimeout, s3.transport.IdleConnTimeout)
	assert.Equal(t, http.DefaultTransport.(*http.Transport).MaxIdleConns, s3.transport.MaxIdleConns)
	assert.NotNil(t, s3.transport.DialContext)
	assert.NotNil(t, s3.transport.Proxy)
}

func TestAbortBackup(t *testing.T) {
	mockServer := newMockS3Server()
	defer mockServer.Close()

	originalBucket := bucket
	originalRoot := root
	defer func() {
		bucket = originalBucket
		root = originalRoot
	}()

	bucket = "test-bucket"
	root = ""

	// Create a mock client
	client := createTestS3Client(mockServer)

	bs := &S3BackupStorage{
		_client: client,
		params:  backupstorage.NoParams(),
	}
	bs.s3SSE.init()

	bh := &S3BackupHandle{
		s3Client: client,
		bs:       bs,
		dir:      "testdir",
		name:     "testbackup",
		readOnly: false,
	}

	// Add a file
	wc, err := bh.AddFile(context.Background(), "testfile", 100)
	require.NoError(t, err)
	wc.Write([]byte("test data"))
	wc.Close()
	bh.waitGroup.Wait()

	// Abort the backup
	err = bh.AbortBackup(context.Background())
	require.NoError(t, err, "AbortBackup() should not error")
}

func TestAddFileWithLargeData(t *testing.T) {
	mockServer := newMockS3Server()
	defer mockServer.Close()

	originalBucket := bucket
	defer func() { bucket = originalBucket }()
	bucket = "test-bucket"

	bh := &S3BackupHandle{
		s3Client: createTestS3Client(mockServer),
		bs: &S3BackupStorage{
			params: backupstorage.NoParams(),
			s3SSE: S3ServerSideEncryption{
				customerAlg: new(string),
				customerKey: new(string),
				customerMd5: new(string),
			},
		},
		readOnly: false,
	}

	// Calculate part size for a large file (10MB)
	largeFileSize := int64(10 * 1024 * 1024)
	wc, err := bh.AddFile(context.Background(), "largefile", largeFileSize)
	require.NoError(t, err, "AddFile() should not error for large file")
	require.NotNil(t, wc)

	// Write some data
	data := make([]byte, 1024)
	n, err := wc.Write(data)
	require.NoError(t, err)
	require.Equal(t, 1024, n)

	err = wc.Close()
	require.NoError(t, err)

	bh.waitGroup.Wait()
	require.False(t, bh.HasErrors())
}

func TestAddFilePartSizeCalculation(t *testing.T) {
	mockServer := newMockS3Server()
	defer mockServer.Close()

	originalBucket := bucket
	originalMinPartSize := minPartSize
	defer func() {
		bucket = originalBucket
		minPartSize = originalMinPartSize
	}()

	bucket = "test-bucket"
	minPartSize = 10 * 1024 * 1024 // 10MB minimum

	bh := &S3BackupHandle{
		s3Client: createTestS3Client(mockServer),
		bs: &S3BackupStorage{
			params: backupstorage.NoParams(),
			s3SSE:  S3ServerSideEncryption{},
		},
		readOnly: false,
	}

	// Small file should use minimum part size
	fileSize := int64(5 * 1024 * 1024) // 5MB
	wc, err := bh.AddFile(context.Background(), "smallfile", fileSize)
	require.NoError(t, err)
	require.NotNil(t, wc)

	wc.Write([]byte("test"))
	wc.Close()
	bh.waitGroup.Wait()
}

func TestAddFileInvalidPartSize(t *testing.T) {
	originalMinPartSize := minPartSize
	defer func() { minPartSize = originalMinPartSize }()

	minPartSize = 10 * 1024 * 1024 * 1024 // 10GB - too large

	bh := &S3BackupHandle{
		s3Client: &s3.Client{},
		bs:       &S3BackupStorage{params: backupstorage.NoParams()},
		readOnly: false,
	}

	_, err := bh.AddFile(context.Background(), "testfile", 100)
	require.Error(t, err, "AddFile() should error with invalid part size")
	require.Contains(t, err.Error(), "minimum S3 part size")
}

func TestServerSideEncryptionConversion(t *testing.T) {
	mockServer := newMockS3Server()
	defer mockServer.Close()

	originalBucket := bucket
	originalSSE := sse
	defer func() {
		bucket = originalBucket
		sse = originalSSE
	}()

	bucket = "test-bucket"
	sse = "aws:kms"

	s3SSE := S3ServerSideEncryption{}
	err := s3SSE.init()
	require.NoError(t, err)

	bh := &S3BackupHandle{
		s3Client: createTestS3Client(mockServer),
		bs: &S3BackupStorage{
			params: backupstorage.NoParams(),
			s3SSE:  s3SSE,
		},
		readOnly: false,
	}

	wc, err := bh.AddFile(context.Background(), "encrypted-file", 100)
	require.NoError(t, err)

	wc.Write([]byte("encrypted data"))
	wc.Close()
	bh.waitGroup.Wait()

	require.False(t, bh.HasErrors())
}

func TestReadFileError(t *testing.T) {
	mockServer := newMockS3Server()
	defer mockServer.Close()

	// Set the mock server to return errors
	mockServer.SetError(true, 0)

	originalBucket := bucket
	defer func() { bucket = originalBucket }()
	bucket = "test-bucket"

	bh := &S3BackupHandle{
		s3Client: createTestS3Client(mockServer),
		bs: &S3BackupStorage{
			params: backupstorage.NoParams(),
			s3SSE:  S3ServerSideEncryption{},
		},
		dir:      "testdir",
		name:     "testbackup",
		readOnly: true,
	}

	// Try to read a file from a server that's returning errors
	_, err := bh.ReadFile(context.Background(), "nonexistent")
	require.Error(t, err, "ReadFile() should error when server returns error")
}

func TestListBackups(t *testing.T) {
	mockServer := newMockS3Server()
	defer mockServer.Close()

	originalBucket := bucket
	originalRoot := root
	defer func() {
		bucket = originalBucket
		root = originalRoot
	}()

	bucket = "test-bucket"
	root = "backups"

	client := createTestS3Client(mockServer)
	bs := &S3BackupStorage{
		_client: client,
		params:  backupstorage.NoParams(),
	}
	bs.s3SSE.init()

	// Update mock to return proper list response
	mockServer.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && strings.Contains(r.URL.RawQuery, "list-type=2") {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
	<Name>test-bucket</Name>
	<Prefix>backups/testdir/</Prefix>
	<Delimiter>/</Delimiter>
	<IsTruncated>false</IsTruncated>
	<CommonPrefixes>
		<Prefix>backups/testdir/backup1/</Prefix>
	</CommonPrefixes>
	<CommonPrefixes>
		<Prefix>backups/testdir/backup2/</Prefix>
	</CommonPrefixes>
</ListBucketResult>`))
		} else {
			w.WriteHeader(http.StatusOK)
		}
	})

	backups, err := bs.ListBackups(context.Background(), "testdir")
	require.NoError(t, err, "ListBackups() should not error")
	require.Len(t, backups, 2, "Should return 2 backups")
	require.Equal(t, "backup1", backups[0].Name())
	require.Equal(t, "backup2", backups[1].Name())
	require.Equal(t, "testdir", backups[0].Directory())
}

func TestListBackupsRoot(t *testing.T) {
	mockServer := newMockS3Server()
	defer mockServer.Close()

	originalBucket := bucket
	originalRoot := root
	defer func() {
		bucket = originalBucket
		root = originalRoot
	}()

	bucket = "test-bucket"
	root = ""

	client := createTestS3Client(mockServer)
	bs := &S3BackupStorage{
		_client: client,
		params:  backupstorage.NoParams(),
	}
	bs.s3SSE.init()

	mockServer.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && strings.Contains(r.URL.RawQuery, "list-type=2") {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
	<Name>test-bucket</Name>
	<IsTruncated>false</IsTruncated>
	<CommonPrefixes>
		<Prefix>root-backup1/</Prefix>
	</CommonPrefixes>
</ListBucketResult>`))
		} else {
			w.WriteHeader(http.StatusOK)
		}
	})

	backups, err := bs.ListBackups(context.Background(), "/")
	require.NoError(t, err, "ListBackups() should not error")
	require.Len(t, backups, 1)
	require.Equal(t, "root-backup1", backups[0].Name())
}

func TestListBackupsPagination(t *testing.T) {
	mockServer := newMockS3Server()
	defer mockServer.Close()

	originalBucket := bucket
	defer func() { bucket = originalBucket }()
	bucket = "test-bucket"

	client := createTestS3Client(mockServer)
	bs := &S3BackupStorage{
		_client: client,
		params:  backupstorage.NoParams(),
	}
	bs.s3SSE.init()

	requestCount := 0
	mockServer.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && strings.Contains(r.URL.RawQuery, "list-type=2") {
			requestCount++
			if requestCount == 1 {
				// First page
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
	<Name>test-bucket</Name>
	<IsTruncated>true</IsTruncated>
	<NextContinuationToken>token123</NextContinuationToken>
	<CommonPrefixes>
		<Prefix>backup1/</Prefix>
	</CommonPrefixes>
</ListBucketResult>`))
			} else {
				// Second page
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
	<Name>test-bucket</Name>
	<IsTruncated>false</IsTruncated>
	<CommonPrefixes>
		<Prefix>backup2/</Prefix>
	</CommonPrefixes>
</ListBucketResult>`))
			}
		} else {
			w.WriteHeader(http.StatusOK)
		}
	})

	backups, err := bs.ListBackups(context.Background(), "testdir")
	require.NoError(t, err)
	require.Len(t, backups, 2)
	require.Equal(t, 2, requestCount, "Should make 2 requests for pagination")
}

func TestStartBackup(t *testing.T) {
	mockServer := newMockS3Server()
	defer mockServer.Close()

	originalBucket := bucket
	defer func() { bucket = originalBucket }()
	bucket = "test-bucket"

	client := createTestS3Client(mockServer)
	bs := &S3BackupStorage{
		_client: client,
		params:  backupstorage.NoParams(),
	}
	bs.s3SSE.init()

	bh, err := bs.StartBackup(context.Background(), "testdir", "newbackup")
	require.NoError(t, err, "StartBackup() should not error")
	require.NotNil(t, bh)

	handle, ok := bh.(*S3BackupHandle)
	require.True(t, ok)
	require.Equal(t, "testdir", handle.dir)
	require.Equal(t, "newbackup", handle.name)
	require.False(t, handle.readOnly)
	require.NotNil(t, handle.s3Client)
}

func TestRemoveBackup(t *testing.T) {
	mockServer := newMockS3Server()
	defer mockServer.Close()

	originalBucket := bucket
	defer func() { bucket = originalBucket }()
	bucket = "test-bucket"

	client := createTestS3Client(mockServer)
	bs := &S3BackupStorage{
		_client: client,
		params:  backupstorage.NoParams(),
	}
	bs.s3SSE.init()

	mockServer.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && strings.Contains(r.URL.RawQuery, "list-type=2") {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
	<Name>test-bucket</Name>
	<IsTruncated>false</IsTruncated>
	<Contents>
		<Key>testdir/backup1/file1.txt</Key>
	</Contents>
	<Contents>
		<Key>testdir/backup1/file2.txt</Key>
	</Contents>
</ListBucketResult>`))
		} else if r.Method == "POST" && strings.Contains(r.URL.RawQuery, "delete") {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult>
</DeleteResult>`))
		} else {
			w.WriteHeader(http.StatusOK)
		}
	})

	err := bs.RemoveBackup(context.Background(), "testdir", "backup1")
	require.NoError(t, err, "RemoveBackup() should not error")
}

func TestRemoveBackupWithErrors(t *testing.T) {
	mockServer := newMockS3Server()
	defer mockServer.Close()

	originalBucket := bucket
	defer func() { bucket = originalBucket }()
	bucket = "test-bucket"

	client := createTestS3Client(mockServer)
	bs := &S3BackupStorage{
		_client: client,
		params:  backupstorage.NoParams(),
	}
	bs.s3SSE.init()

	mockServer.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && strings.Contains(r.URL.RawQuery, "list-type=2") {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
	<Name>test-bucket</Name>
	<IsTruncated>false</IsTruncated>
	<Contents>
		<Key>testdir/backup1/file1.txt</Key>
	</Contents>
</ListBucketResult>`))
		} else if r.Method == "POST" && strings.Contains(r.URL.RawQuery, "delete") {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult>
	<Error>
		<Key>testdir/backup1/file1.txt</Key>
		<Code>AccessDenied</Code>
		<Message>Access Denied</Message>
	</Error>
</DeleteResult>`))
		} else {
			w.WriteHeader(http.StatusOK)
		}
	})

	err := bs.RemoveBackup(context.Background(), "testdir", "backup1")
	require.Error(t, err, "RemoveBackup() should error when delete fails")
	require.Contains(t, err.Error(), "Access Denied")
}

func TestRemoveBackupPaginated(t *testing.T) {
	mockServer := newMockS3Server()
	defer mockServer.Close()

	originalBucket := bucket
	defer func() { bucket = originalBucket }()
	bucket = "test-bucket"

	client := createTestS3Client(mockServer)
	bs := &S3BackupStorage{
		_client: client,
		params:  backupstorage.NoParams(),
	}
	bs.s3SSE.init()

	requestCount := 0
	mockServer.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && strings.Contains(r.URL.RawQuery, "list-type=2") {
			requestCount++
			if requestCount == 1 {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
	<Name>test-bucket</Name>
	<IsTruncated>true</IsTruncated>
	<NextContinuationToken>token456</NextContinuationToken>
	<Contents>
		<Key>testdir/backup1/file1.txt</Key>
	</Contents>
</ListBucketResult>`))
			} else {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
	<Name>test-bucket</Name>
	<IsTruncated>false</IsTruncated>
	<Contents>
		<Key>testdir/backup1/file2.txt</Key>
	</Contents>
</ListBucketResult>`))
			}
		} else if r.Method == "POST" && strings.Contains(r.URL.RawQuery, "delete") {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult>
</DeleteResult>`))
		} else {
			w.WriteHeader(http.StatusOK)
		}
	})

	err := bs.RemoveBackup(context.Background(), "testdir", "backup1")
	require.NoError(t, err)
	require.Equal(t, 2, requestCount, "Should handle paginated list for removal")
}

func TestClientCaching(t *testing.T) {
	mockServer := newMockS3Server()
	defer mockServer.Close()

	originalBucket := bucket
	defer func() { bucket = originalBucket }()
	bucket = "test-bucket"

	client := createTestS3Client(mockServer)

	bs := &S3BackupStorage{
		_client: client,
		params:  backupstorage.NoParams(),
	}

	// Calling client() should return the cached client
	client2, err := bs.client()
	require.NoError(t, err)
	require.Same(t, client, client2, "client() should return cached client")
}

func TestClientInitializationEmptyBucket(t *testing.T) {
	originalBucket := bucket
	defer func() { bucket = originalBucket }()

	bucket = ""

	bs := &S3BackupStorage{
		params: backupstorage.NoParams(),
	}

	_, err := bs.client()
	require.Error(t, err, "client() should error with empty bucket")
	require.Contains(t, err.Error(), "--s3-backup-storage-bucket required")
}

func TestListBackupsError(t *testing.T) {
	mockServer := newMockS3Server()
	defer mockServer.Close()

	mockServer.SetError(true, 0)

	originalBucket := bucket
	defer func() { bucket = originalBucket }()
	bucket = "test-bucket"

	client := createTestS3Client(mockServer)
	bs := &S3BackupStorage{
		_client: client,
		params:  backupstorage.NoParams(),
	}
	bs.s3SSE.init()

	_, err := bs.ListBackups(context.Background(), "testdir")
	require.Error(t, err, "ListBackups() should error when server returns error")
}

func TestStartBackupError(t *testing.T) {
	bs := &S3BackupStorage{
		params: backupstorage.NoParams(),
	}

	// client() will error because _client is nil and bucket validation will fail
	originalBucket := bucket
	bucket = ""
	defer func() { bucket = originalBucket }()

	_, err := bs.StartBackup(context.Background(), "testdir", "newbackup")
	require.Error(t, err, "StartBackup() should error when client init fails")
}

func TestRemoveBackupListError(t *testing.T) {
	mockServer := newMockS3Server()
	defer mockServer.Close()

	mockServer.SetError(true, 0)

	originalBucket := bucket
	defer func() { bucket = originalBucket }()
	bucket = "test-bucket"

	client := createTestS3Client(mockServer)
	bs := &S3BackupStorage{
		_client: client,
		params:  backupstorage.NoParams(),
	}

	err := bs.RemoveBackup(context.Background(), "testdir", "backup1")
	require.Error(t, err, "RemoveBackup() should error when list fails")
}

func TestRemoveBackupDeleteError(t *testing.T) {
	mockServer := newMockS3Server()
	defer mockServer.Close()

	originalBucket := bucket
	defer func() { bucket = originalBucket }()
	bucket = "test-bucket"

	client := createTestS3Client(mockServer)
	bs := &S3BackupStorage{
		_client: client,
		params:  backupstorage.NoParams(),
	}

	mockServer.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && strings.Contains(r.URL.RawQuery, "list-type=2") {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
	<Name>test-bucket</Name>
	<IsTruncated>false</IsTruncated>
	<Contents>
		<Key>testdir/backup1/file1.txt</Key>
	</Contents>
</ListBucketResult>`))
		} else if r.Method == "POST" && strings.Contains(r.URL.RawQuery, "delete") {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<Error>
	<Code>InternalError</Code>
	<Message>Internal Error</Message>
</Error>`))
		} else {
			w.WriteHeader(http.StatusOK)
		}
	})

	err := bs.RemoveBackup(context.Background(), "testdir", "backup1")
	require.Error(t, err, "RemoveBackup() should error when delete fails")
}

func TestGetLogLevel(t *testing.T) {
	originalLogLevel := requiredLogLevel
	defer func() { requiredLogLevel = originalLogLevel }()

	// Test valid log level
	requiredLogLevel = "LogDebug"
	level := getLogLevel()
	require.NotEqual(t, aws.ClientLogMode(0), level)

	// Test invalid log level (should return default)
	requiredLogLevel = "InvalidLogLevel"
	level = getLogLevel()
	require.Equal(t, aws.ClientLogMode(0), level)
}

func TestEndpointResolver(t *testing.T) {
	originalEndpoint := endpoint
	defer func() { endpoint = originalEndpoint }()

	endpoint = "https://custom-s3.example.com"

	resolver := newEndpointResolver()
	require.NotNil(t, resolver)
	require.Equal(t, &endpoint, resolver.endpoint)

	// Test ResolveEndpoint
	regionStr := "us-east-1"
	params := s3.EndpointParameters{
		Region: &regionStr,
	}
	resolvedEndpoint, err := resolver.ResolveEndpoint(context.Background(), params)
	require.NoError(t, err)
	require.NotEmpty(t, resolvedEndpoint.URI.String())
}

func TestRetryerMethods(t *testing.T) {
	stdRetryer := retry.NewStandard(func(o *retry.StandardOptions) {
		o.MaxAttempts = 3
	})

	retryer := &ClosedConnectionRetryer{
		awsRetryer: stdRetryer,
	}

	// Test RetryDelay
	delay, err := retryer.RetryDelay(1, nil)
	require.NoError(t, err)
	require.GreaterOrEqual(t, delay, time.Duration(0))

	// Test GetRetryToken
	ctx := context.Background()
	releaseFunc, err := retryer.GetRetryToken(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, releaseFunc)
	if releaseFunc != nil {
		err = releaseFunc(nil)
		require.NoError(t, err)
	}

	// Test GetInitialToken
	token := retryer.GetInitialToken()
	require.NotNil(t, token)
	if token != nil {
		err := token(nil)
		require.NoError(t, err)
	}
}

func TestFullBackupRestoreWorkflow(t *testing.T) {
	mockServer := newMockS3Server()
	defer mockServer.Close()

	originalBucket := bucket
	originalRoot := root
	defer func() {
		bucket = originalBucket
		root = originalRoot
	}()

	bucket = "test-bucket"
	root = "backups"

	client := createTestS3Client(mockServer)
	bs := &S3BackupStorage{
		_client: client,
		params:  backupstorage.Params{Logger: logutil.NewMemoryLogger(), Stats: stats.NewFakeStats()},
	}
	bs.s3SSE.init()

	// Start a new backup
	bh, err := bs.StartBackup(context.Background(), "testdir", "full-backup")
	require.NoError(t, err)
	require.False(t, bh.(*S3BackupHandle).readOnly)

	// Add multiple files
	testData1 := []byte("file 1 contents")
	testData2 := []byte("file 2 contents with more data")

	wc1, err := bh.AddFile(context.Background(), "file1.dat", int64(len(testData1)))
	require.NoError(t, err)
	_, err = wc1.Write(testData1)
	require.NoError(t, err)
	err = wc1.Close()
	require.NoError(t, err)

	wc2, err := bh.AddFile(context.Background(), "file2.dat", int64(len(testData2)))
	require.NoError(t, err)
	_, err = wc2.Write(testData2)
	require.NoError(t, err)
	err = wc2.Close()
	require.NoError(t, err)

	// End the backup
	err = bh.EndBackup(context.Background())
	require.NoError(t, err)

	// Close and reopen storage
	err = bs.Close()
	require.NoError(t, err)

	bs2 := &S3BackupStorage{
		_client: client,
		params:  backupstorage.Params{Logger: logutil.NewMemoryLogger(), Stats: stats.NewFakeStats()},
	}
	bs2.s3SSE.init()

	// Set up mock to return proper list for ListBackups
	mockServer.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && strings.Contains(r.URL.RawQuery, "list-type=2") {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
	<Name>test-bucket</Name>
	<IsTruncated>false</IsTruncated>
	<CommonPrefixes>
		<Prefix>backups/testdir/full-backup/</Prefix>
	</CommonPrefixes>
</ListBucketResult>`))
		} else if r.Method == "GET" && !strings.Contains(r.URL.RawQuery, "list-type") {
			// GetObject
			path := r.URL.Path
			mockServer.mu.Lock()
			parts := mockServer.uploadedParts[path]
			mockServer.mu.Unlock()

			if len(parts) > 0 {
				w.WriteHeader(http.StatusOK)
				for _, part := range parts {
					w.Write(part)
				}
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		} else {
			w.WriteHeader(http.StatusOK)
		}
	})

	// List backups
	backups, err := bs2.ListBackups(context.Background(), "testdir")
	require.NoError(t, err)
	require.Len(t, backups, 1)
	require.Equal(t, "full-backup", backups[0].Name())

	// Read files back
	readBh := backups[0]
	require.True(t, readBh.(*S3BackupHandle).readOnly)

	rc1, err := readBh.ReadFile(context.Background(), "file1.dat")
	require.NoError(t, err)
	data1, err := io.ReadAll(rc1)
	require.NoError(t, err)
	require.Equal(t, testData1, data1)
	rc1.Close()

	rc2, err := readBh.ReadFile(context.Background(), "file2.dat")
	require.NoError(t, err)
	data2, err := io.ReadAll(rc2)
	require.NoError(t, err)
	require.Equal(t, testData2, data2)
	rc2.Close()
}

func TestSSEWithActualUpload(t *testing.T) {
	mockServer := newMockS3Server()
	defer mockServer.Close()

	originalBucket := bucket
	originalSSE := sse
	defer func() {
		bucket = originalBucket
		sse = originalSSE
	}()

	bucket = "test-bucket"
	sse = "AES256"

	client := createTestS3Client(mockServer)

	s3SSE := S3ServerSideEncryption{}
	err := s3SSE.init()
	require.NoError(t, err)
	require.Equal(t, types.ServerSideEncryption("AES256"), s3SSE.awsAlg)

	bs := &S3BackupStorage{
		_client: client,
		params:  backupstorage.NoParams(),
		s3SSE:   s3SSE,
	}

	bh, err := bs.StartBackup(context.Background(), "encrypted", "backup1")
	require.NoError(t, err)

	wc, err := bh.AddFile(context.Background(), "secret.txt", 100)
	require.NoError(t, err)

	_, err = wc.Write([]byte("secret data"))
	require.NoError(t, err)

	err = wc.Close()
	require.NoError(t, err)

	err = bh.EndBackup(context.Background())
	require.NoError(t, err)
}

func TestObjName(t *testing.T) {
	originalRoot := root
	defer func() { root = originalRoot }()

	// Test without root
	root = ""
	result := objName("dir1", "dir2", "file.txt")
	require.Equal(t, "dir1/dir2/file.txt", result)

	// Test with root
	root = "backup-root"
	result = objName("dir1", "dir2", "file.txt")
	require.Equal(t, "backup-root/dir1/dir2/file.txt", result)

	// Test with empty parts
	result = objName()
	require.Equal(t, "backup-root/", result)

	// Test with single part
	result = objName("single")
	require.Equal(t, "backup-root/single", result)
}

func TestS3BackupStorageWithParams(t *testing.T) {
	bs := newS3BackupStorage()

	newParams := backupstorage.Params{
		Logger: logutil.NewMemoryLogger(),
		Stats:  stats.NewFakeStats(),
	}

	newBS := bs.WithParams(newParams).(*S3BackupStorage)
	require.NotNil(t, newBS)
	require.Equal(t, newParams.Logger, newBS.params.Logger)
	require.Equal(t, newParams.Stats, newBS.params.Stats)
	require.NotNil(t, newBS.transport)
}

func TestS3BackupStorageClose(t *testing.T) {
	bs := &S3BackupStorage{
		_client: &s3.Client{},
	}

	err := bs.Close()
	require.NoError(t, err)
	require.Nil(t, bs._client)
}

func TestCalculateUploadPartSize(t *testing.T) {
	originalMinimum := minPartSize
	defer func() { minPartSize = originalMinimum }()

	tests := []struct {
		name            string
		filesize        int64
		minimumPartSize int64
		want            int64
		err             error
	}{
		{
			name:            "minimum - 10 MiB",
			filesize:        1024 * 1024 * 10, // 10 MiB
			minimumPartSize: 1024 * 1024 * 5,  // 5 MiB
			want:            1024 * 1024 * 5,  // 5 MiB,
			err:             nil,
		},
		{
			name:            "below minimum - 10 MiB",
			filesize:        1024 * 1024 * 10, // 10 MiB
			minimumPartSize: 1024 * 1024 * 8,  // 8 MiB
			want:            1024 * 1024 * 8,  // 8 MiB,
			err:             nil,
		},
		{
			name:            "above minimum - 1 TiB",
			filesize:        1024 * 1024 * 1024 * 1024, // 1 TiB
			minimumPartSize: 1024 * 1024 * 5,           // 5 MiB
			want:            109951163,                 // ~104 MiB
			err:             nil,
		},
		{
			name:            "below minimum - 1 TiB",
			filesize:        1024 * 1024 * 1024 * 1024, // 1 TiB
			minimumPartSize: 1024 * 1024 * 200,         // 200 MiB
			want:            1024 * 1024 * 200,         // 200 MiB
			err:             nil,
		},
		{
			name:            "below S3 limits - 5 MiB",
			filesize:        1024 * 1024 * 3, // 3 MiB
			minimumPartSize: 1024 * 1024 * 4, // 4 MiB
			want:            1024 * 1024 * 5, // 5 MiB - should always return the minimum
			err:             nil,
		},
		{
			name:            "above S3 limits - 5 GiB",
			filesize:        1024 * 1024 * 1024 * 1024, // 1 TiB
			minimumPartSize: 1024 * 1024 * 1024 * 6,    // 6 GiB
			want:            0,
			err:             ErrPartSize,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			minPartSize = tt.minimumPartSize
			partSize, err := calculateUploadPartSize(tt.filesize)
			require.ErrorIs(t, err, tt.err)
			require.Equal(t, tt.want, partSize)
		})
	}
}

/*
Copyright 2026 The Vitess Authors.

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

package s3backupstorage

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	handler       http.Handler
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
		handler := m.handler
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

		if handler != nil {
			handler.ServeHTTP(w, r)
			return
		}

		m.serveDefault(w, r)
	}))

	return m
}

func (m *mockS3Server) serveDefault(w http.ResponseWriter, r *http.Request) {
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
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
	<IsTruncated>false</IsTruncated>
	<Contents>
		<Key>test-key</Key>
		<ETag>"test-etag"</ETag>
	</Contents>
</ListBucketResult>`))
	} else if r.Method == "POST" && strings.Contains(r.URL.RawQuery, "delete") {
		// DeleteObjects
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult>
</DeleteResult>`))
	} else {
		w.WriteHeader(http.StatusOK)
	}
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

func (m *mockS3Server) SetHandler(handler http.Handler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handler = handler
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

func requireTimedIncrementAtLeast(t *testing.T, fakeStats *stats.FakeStats, min time.Duration) {
	t.Helper()
	require.NotEmpty(t, fakeStats.TimedIncrementCalls)

	for _, call := range fakeStats.TimedIncrementCalls {
		if call >= min {
			return
		}
	}

	require.Failf(t, "expected timed increment meeting minimum", "expected at least one TimedIncrement >= %s, got %v", min, fakeStats.TimedIncrementCalls)
}

func setSSEForTest(t *testing.T, value string) {
	t.Helper()
	originalSSE := sse
	sse = value
	t.Cleanup(func() {
		sse = originalSSE
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

	wc, err := bh.AddFile(t.Context(), "somefile", 100000)
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
	mockServer.SetDelay(10 * time.Millisecond)

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
		wc, err := bh.AddFile(t.Context(), fmt.Sprintf("somefile-%d", i), 100000)
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
	requireTimedIncrementAtLeast(t, scopedStats, 10*time.Millisecond)
	require.Len(t, scopedStats.TimedIncrementBytesCalls, 0)
}

func TestAddFileErrorStats(t *testing.T) {
	mockServer := newMockS3Server()
	defer mockServer.Close()

	mockServer.SetDelay(10 * time.Millisecond)
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

	wc, err := bh.AddFile(t.Context(), "somefile", 100000)
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
	requireTimedIncrementAtLeast(t, scopedStats, 10*time.Millisecond)
	require.Len(t, scopedStats.TimedIncrementBytesCalls, 0)
}

func TestAddFileMultipartStats(t *testing.T) {
	mockServer := newMockS3Server()
	defer mockServer.Close()
	mockServer.SetDelay(10 * time.Millisecond)

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

	data := bytes.Repeat([]byte("a"), 6*1024*1024)

	wc, err := bh.AddFile(t.Context(), "multipart-file", int64(len(data)))
	require.NoError(t, err)

	n, err := wc.Write(data)
	require.NoError(t, err)
	require.Equal(t, len(data), n)

	err = wc.Close()
	require.NoError(t, err)

	bh.waitGroup.Wait()

	require.False(t, bh.HasErrors(), "AddFile() should not have recorded errors")
	require.Len(t, fakeStats.ScopeCalls, 1)

	scopedStats := fakeStats.ScopeReturns[0]
	require.Len(t, scopedStats.ScopeV, 1)
	require.Equal(t, scopedStats.ScopeV[stats.ScopeOperation], "AWS:Request:Send")
	require.Greater(t, len(scopedStats.TimedIncrementCalls), 1)
	requireTimedIncrementAtLeast(t, scopedStats, 10*time.Millisecond)
	require.Len(t, scopedStats.TimedIncrementBytesCalls, 0)
}

func TestReadFileStats(t *testing.T) {
	mockServer := newMockS3Server()
	defer mockServer.Close()
	mockServer.SetDelay(10 * time.Millisecond)

	originalBucket := bucket
	originalRoot := root
	defer func() {
		bucket = originalBucket
		root = originalRoot
	}()

	bucket = "test-bucket"
	root = ""

	fakeStats := stats.NewFakeStats()

	writeBh := &S3BackupHandle{
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

	wc, err := writeBh.AddFile(t.Context(), "testfile", 100)
	require.NoError(t, err)
	_, err = wc.Write([]byte("test file contents"))
	require.NoError(t, err)
	require.NoError(t, wc.Close())
	writeBh.waitGroup.Wait()

	readBh := &S3BackupHandle{
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
		dir:      "testdir",
		name:     "testbackup",
		readOnly: true,
	}

	rc, err := readBh.ReadFile(t.Context(), "testfile")
	require.NoError(t, err)
	_, err = io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())

	require.Len(t, fakeStats.ScopeCalls, 1)
	scopedStats := fakeStats.ScopeReturns[0]
	require.Len(t, scopedStats.ScopeV, 1)
	require.Equal(t, scopedStats.ScopeV[stats.ScopeOperation], "AWS:Request:Send")
	requireTimedIncrementAtLeast(t, scopedStats, 10*time.Millisecond)
	require.Len(t, scopedStats.TimedIncrementBytesCalls, 0)
}

func TestReadFileErrorStats(t *testing.T) {
	mockServer := newMockS3Server()
	defer mockServer.Close()
	mockServer.SetDelay(10 * time.Millisecond)
	mockServer.SetError(true, 0)

	originalBucket := bucket
	originalRoot := root
	defer func() {
		bucket = originalBucket
		root = originalRoot
	}()

	bucket = "test-bucket"
	root = ""

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
		dir:      "testdir",
		name:     "testbackup",
		readOnly: true,
	}

	_, err := bh.ReadFile(t.Context(), "testfile")
	require.Error(t, err)

	require.Len(t, fakeStats.ScopeCalls, 1)
	scopedStats := fakeStats.ScopeReturns[0]
	require.Len(t, scopedStats.ScopeV, 1)
	require.Equal(t, scopedStats.ScopeV[stats.ScopeOperation], "AWS:Request:Send")
	requireTimedIncrementAtLeast(t, scopedStats, 10*time.Millisecond)
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

	wc, err := bh.AddFile(t.Context(), "testfile", 100)
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

	rc, err := readBh.ReadFile(t.Context(), "testfile")
	require.NoError(t, err, "ReadFile() should not error")
	require.NotNil(t, rc, "ReadFile() should return non-nil ReadCloser")

	data, err := io.ReadAll(rc)
	require.NoError(t, err, "ReadAll() should not error")
	require.Equal(t, testData, data, "Read data should match uploaded data")

	err = rc.Close()
	require.NoError(t, err, "Close() should not error")
}

func TestReadFileInvalidDownloadFlags(t *testing.T) {
	bh := &S3BackupHandle{
		bs: &S3BackupStorage{
			params: backupstorage.NoParams(),
			s3SSE:  S3ServerSideEncryption{},
		},
		readOnly: true,
	}

	origPartSize := downloadPartSize
	origConcurrency := downloadConcurrency
	origMinPartSize := minPartSize
	defer func() {
		downloadPartSize = origPartSize
		downloadConcurrency = origConcurrency
		minPartSize = origMinPartSize
	}()

	// Part size below download minimum (5MiB)
	downloadPartSize = 1024
	downloadConcurrency = 5
	_, err := bh.ReadFile(t.Context(), "testfile")
	require.Error(t, err)
	require.Contains(t, err.Error(), "5 MiB")

	// Negative part size
	downloadPartSize = -1
	_, err = bh.ReadFile(t.Context(), "testfile")
	require.Error(t, err)
	require.Contains(t, err.Error(), "5 MiB")

	// Zero concurrency
	downloadPartSize = 8 * 1024 * 1024
	downloadConcurrency = 0
	_, err = bh.ReadFile(t.Context(), "testfile")
	require.Error(t, err)
	require.Contains(t, err.Error(), ">= 1")

	// Regression: download part size (8MiB) is valid even when upload minPartSize
	// is set higher (16MiB). The two thresholds must be independent.
	// We need a real s3Client to get past validation without a nil-pointer panic.
	minPartSize = 16 * 1024 * 1024
	downloadPartSize = 8 * 1024 * 1024
	downloadConcurrency = 5

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	originalBucket := bucket
	originalRoot := root
	defer func() {
		bucket = originalBucket
		root = originalRoot
	}()
	bucket = "test-bucket"
	root = ""

	bhWithClient := &S3BackupHandle{
		s3Client: s3.New(s3.Options{
			Region:       "us-east-1",
			BaseEndpoint: aws.String(server.URL),
			UsePathStyle: true,
			Credentials: aws.CredentialsProviderFunc(func(_ context.Context) (aws.Credentials, error) {
				return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
			}),
			Retryer: func() aws.Retryer {
				return retry.NewStandard(func(o *retry.StandardOptions) { o.MaxAttempts = 1 })
			}(),
		}),
		bs: &S3BackupStorage{
			params: backupstorage.NoParams(),
			s3SSE:  S3ServerSideEncryption{},
		},
		readOnly: true,
	}

	_, err = bhWithClient.ReadFile(t.Context(), "testfile")
	// Validation passes — error is from S3 (404), not from our part-size check
	require.Error(t, err)
	require.NotContains(t, err.Error(), "5 MiB")
}

func TestReadFileSSECHeaderForwarding(t *testing.T) {
	testData := []byte("sse-c parallel download test data")
	sseAlg := "AES256"
	sseKey := "dGVzdC1lbmNyeXB0aW9uLWtleS0xMjM0NTY3ODk=" // base64 test key
	sseMD5 := "dGVzdC1tZDU="

	// Mock server that rejects HEAD requests without SSE-C headers (like real S3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			if r.Header.Get("X-Amz-Server-Side-Encryption-Customer-Algorithm") == "" {
				w.WriteHeader(http.StatusForbidden)
				w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<Error>
	<Code>AccessDenied</Code>
	<Message>Requests specifying Server Side Encryption with Customer provided keys must provide an appropriate secret key.</Message>
</Error>`))
				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(testData)))
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method == "GET" {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(testData)-1, len(testData)))
			w.WriteHeader(http.StatusOK)
			w.Write(testData)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	originalBucket := bucket
	originalRoot := root
	defer func() {
		bucket = originalBucket
		root = originalRoot
	}()
	bucket = "test-bucket"
	root = ""

	s3Client := s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(server.URL),
		UsePathStyle: true,
		Credentials: aws.CredentialsProviderFunc(func(_ context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		}),
		Retryer: func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) { o.MaxAttempts = 1 })
		}(),
	})

	bh := &S3BackupHandle{
		s3Client: s3Client,
		bs: &S3BackupStorage{
			params: backupstorage.NoParams(),
			s3SSE: S3ServerSideEncryption{
				customerAlg: &sseAlg,
				customerKey: &sseKey,
				customerMd5: &sseMD5,
			},
		},
		dir:      "testdir",
		name:     "testbackup",
		readOnly: true,
	}

	rc, err := bh.ReadFile(t.Context(), "testfile")
	require.NoError(t, err, "ReadFile with SSE-C should succeed via header forwarding workaround")
	require.NotNil(t, rc)

	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, testData, data)
	require.NoError(t, rc.Close())
}

func TestReadFileParallelismSmallReads(t *testing.T) {
	const objectSize = 40 * 1024 * 1024 // 40MiB
	testData := bytes.Repeat([]byte("x"), objectSize)

	var (
		mu          sync.Mutex
		maxInFlight int
		curInFlight int
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			w.Header().Set("Content-Length", strconv.Itoa(len(testData)))
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method == "GET" {
			mu.Lock()
			curInFlight++
			if curInFlight > maxInFlight {
				maxInFlight = curInFlight
			}
			mu.Unlock()

			time.Sleep(20 * time.Millisecond)

			mu.Lock()
			curInFlight--
			mu.Unlock()

			rangeHdr := r.Header.Get("Range")
			if rangeHdr == "" {
				w.Header().Set("Content-Length", strconv.Itoa(len(testData)))
				w.WriteHeader(http.StatusOK)
				w.Write(testData)
				return
			}
			var start, end int
			fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)
			if end >= len(testData) {
				end = len(testData) - 1
			}
			chunk := testData[start : end+1]
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(testData)))
			w.Header().Set("Content-Length", strconv.Itoa(len(chunk)))
			w.WriteHeader(http.StatusPartialContent)
			w.Write(chunk)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	originalBucket := bucket
	originalRoot := root
	origPartSize := downloadPartSize
	origConcurrency := downloadConcurrency
	defer func() {
		bucket = originalBucket
		root = originalRoot
		downloadPartSize = origPartSize
		downloadConcurrency = origConcurrency
	}()
	bucket = "test-bucket"
	root = ""
	downloadPartSize = 8 * 1024 * 1024 // 8MiB parts
	downloadConcurrency = 4            // expect ~4 parallel GETs

	s3Client := s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(server.URL),
		UsePathStyle: true,
		Credentials: aws.CredentialsProviderFunc(func(_ context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		}),
		Retryer: func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) { o.MaxAttempts = 1 })
		}(),
	})

	bh := &S3BackupHandle{
		s3Client: s3Client,
		bs: &S3BackupStorage{
			params: backupstorage.NoParams(),
			s3SSE:  S3ServerSideEncryption{},
		},
		dir:      "testdir",
		name:     "testbackup",
		readOnly: true,
	}

	rc, err := bh.ReadFile(t.Context(), "testfile")
	require.NoError(t, err)

	// Simulate pgzip's decompression pattern: read through a 4 KiB buffer.
	// Without the bufio.Reader wrapper, each 4 KiB read would trigger a full
	// worker pool spin-up in the transfer manager's concurrentReader.
	buf := make([]byte, 4096)
	var totalRead int
	for {
		n, readErr := rc.Read(buf)
		totalRead += n
		if readErr == io.EOF {
			break
		}
		require.NoError(t, readErr)
	}
	require.Equal(t, objectSize, totalRead)
	require.NoError(t, rc.Close())

	assert.GreaterOrEqual(t, maxInFlight, downloadConcurrency-1,
		"expected at least %d parallel GETs, got %d", downloadConcurrency-1, maxInFlight)
}

func TestDownloadBufferSizeValidation(t *testing.T) {
	tests := []struct {
		name        string
		partSize    int64
		concurrency int
		wantErr     string
	}{
		{
			name:        "valid defaults",
			partSize:    8 * 1024 * 1024,
			concurrency: 5,
		},
		{
			name:        "multiplication overflow",
			partSize:    math.MaxInt64,
			concurrency: 2,
			wantErr:     "overflows int64",
		},
		{
			name:        "addition overflow",
			partSize:    math.MaxInt64/2 + 1,
			concurrency: 1,
			wantErr:     "overflows int64",
		},
		{
			name:        "exceeds per-file memory limit",
			partSize:    128 * 1024 * 1024, // 128MiB
			concurrency: 8,                 // window=1GiB + part=128MiB = 1152MiB > 1GiB limit
			wantErr:     "exceeds limit",
		},
		{
			name:        "at boundary - fits exactly",
			partSize:    100 * 1024 * 1024, // 100MiB
			concurrency: 9,                 // window=900MiB + part=100MiB = 1000MiB < 1GiB
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size, err := downloadBufferSize(tt.partSize, tt.concurrency)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.partSize*int64(tt.concurrency), size)
			}
		})
	}
}

// closeTrackingTransport wraps an http.RoundTripper and replaces response
// bodies with a trackingBody that records Close calls.
type closeTrackingTransport struct {
	wrapped    http.RoundTripper
	bodyClosed atomic.Int32
}

func (t *closeTrackingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.wrapped.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	if req.Method == "GET" {
		resp.Body = &trackingBody{ReadCloser: resp.Body, closed: &t.bodyClosed}
	}
	return resp, err
}

type trackingBody struct {
	io.ReadCloser
	closed *atomic.Int32
}

func (b *trackingBody) Close() error {
	b.closed.Add(1)
	return b.ReadCloser.Close()
}

func TestReadFileZeroLengthObjectCloser(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			w.Header().Set("Content-Length", "0")
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method == "GET" {
			w.Header().Set("Content-Length", "0")
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	originalBucket := bucket
	originalRoot := root
	origPartSize := downloadPartSize
	origConcurrency := downloadConcurrency
	defer func() {
		bucket = originalBucket
		root = originalRoot
		downloadPartSize = origPartSize
		downloadConcurrency = origConcurrency
	}()
	bucket = "test-bucket"
	root = ""
	downloadPartSize = 8 * 1024 * 1024
	downloadConcurrency = 5

	// Use a custom transport that tracks response body Close calls
	tracker := &closeTrackingTransport{
		wrapped: &http.Transport{},
	}

	s3Client := s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(server.URL),
		UsePathStyle: true,
		HTTPClient:   &http.Client{Transport: tracker},
		Credentials: aws.CredentialsProviderFunc(func(_ context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		}),
		Retryer: func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) { o.MaxAttempts = 1 })
		}(),
	})

	bh := &S3BackupHandle{
		s3Client: s3Client,
		bs: &S3BackupStorage{
			params: backupstorage.NoParams(),
			s3SSE:  S3ServerSideEncryption{},
		},
		dir:      "testdir",
		name:     "testbackup",
		readOnly: true,
	}

	rc, err := bh.ReadFile(t.Context(), "empty-object")
	require.NoError(t, err)

	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Empty(t, data)

	// Body must not be closed yet — only our explicit Close should trigger it
	assert.Equal(t, int32(0), tracker.bodyClosed.Load(),
		"response body should not be closed before rc.Close()")

	err = rc.Close()
	require.NoError(t, err)

	// After Close, the underlying response body must have been closed
	assert.Equal(t, int32(1), tracker.bodyClosed.Load(),
		"response body should be closed exactly once after rc.Close()")
}

func TestReadFileCoalescesSmallReads(t *testing.T) {
	// This test pins the bufio.Reader coalescing: without it, each 4 KiB read
	// from pgzip would pass directly to the SDK body, resulting in ~10k Read
	// calls for a 40 MiB object. With the bufio.Reader, small reads are batched
	// into part-sized reads on the underlying body.
	const objectSize = 40 * 1024 * 1024 // 40MiB
	testData := bytes.Repeat([]byte("y"), objectSize)

	var bodyReadCalls atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			w.Header().Set("Content-Length", strconv.Itoa(len(testData)))
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method == "GET" {
			rangeHdr := r.Header.Get("Range")
			if rangeHdr == "" {
				w.Header().Set("Content-Length", strconv.Itoa(len(testData)))
				w.WriteHeader(http.StatusOK)
				w.Write(testData)
				return
			}
			var start, end int
			fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)
			if end >= len(testData) {
				end = len(testData) - 1
			}
			chunk := testData[start : end+1]
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(testData)))
			w.Header().Set("Content-Length", strconv.Itoa(len(chunk)))
			w.WriteHeader(http.StatusPartialContent)
			w.Write(chunk)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	originalBucket := bucket
	originalRoot := root
	origPartSize := downloadPartSize
	origConcurrency := downloadConcurrency
	defer func() {
		bucket = originalBucket
		root = originalRoot
		downloadPartSize = origPartSize
		downloadConcurrency = origConcurrency
	}()
	bucket = "test-bucket"
	root = ""
	downloadPartSize = 8 * 1024 * 1024
	downloadConcurrency = 5

	// Use a custom transport that counts Read calls on response bodies
	countingTransport := &readCountingTransport{
		wrapped:   &http.Transport{},
		readCalls: &bodyReadCalls,
	}

	s3Client := s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(server.URL),
		UsePathStyle: true,
		HTTPClient:   &http.Client{Transport: countingTransport},
		Credentials: aws.CredentialsProviderFunc(func(_ context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		}),
		Retryer: func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) { o.MaxAttempts = 1 })
		}(),
	})

	bh := &S3BackupHandle{
		s3Client: s3Client,
		bs: &S3BackupStorage{
			params: backupstorage.NoParams(),
			s3SSE:  S3ServerSideEncryption{},
		},
		dir:      "testdir",
		name:     "testbackup",
		readOnly: true,
	}

	rc, err := bh.ReadFile(t.Context(), "testfile")
	require.NoError(t, err)

	// Read with 4 KiB buffer (simulating pgzip)
	buf := make([]byte, 4096)
	var totalRead int
	for {
		n, readErr := rc.Read(buf)
		totalRead += n
		if readErr == io.EOF {
			break
		}
		require.NoError(t, readErr)
	}
	require.Equal(t, objectSize, totalRead)
	require.NoError(t, rc.Close())

	// Without bufio.Reader: ~10k Read calls (40MiB / 4KiB).
	// With bufio.Reader: the underlying body sees far fewer reads (order of
	// objectSize/partSize = ~5 range responses, each read in part-sized chunks).
	// We assert the body saw << 1000 reads to pin that coalescing is working.
	reads := bodyReadCalls.Load()
	assert.Less(t, reads, int64(1000),
		"expected coalesced reads on underlying body (<1000), got %d — bufio.Reader may be missing", reads)
}

// readCountingTransport counts Read() calls on GET response bodies.
type readCountingTransport struct {
	wrapped   http.RoundTripper
	readCalls *atomic.Int64
}

func (t *readCountingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.wrapped.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	if req.Method == "GET" {
		resp.Body = &readCountingBody{ReadCloser: resp.Body, calls: t.readCalls}
	}
	return resp, err
}

type readCountingBody struct {
	io.ReadCloser
	calls *atomic.Int64
}

func (b *readCountingBody) Read(p []byte) (int, error) {
	b.calls.Add(1)
	return b.ReadCloser.Read(p)
}

func TestReadFileMidStreamError(t *testing.T) {
	// Verifies that a non-EOF error from a later range GET surfaces to the caller.
	const objectSize = 24 * 1024 * 1024 // 24MiB (3 parts at 8MiB)

	var rangeRequests atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			w.Header().Set("Content-Length", strconv.Itoa(objectSize))
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method == "GET" {
			reqNum := rangeRequests.Add(1)
			// Fail the 3rd range request
			if reqNum >= 3 {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>InternalError</Code><Message>simulated failure</Message></Error>`))
				return
			}
			rangeHdr := r.Header.Get("Range")
			var start, end int
			fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)
			if end >= objectSize {
				end = objectSize - 1
			}
			chunk := bytes.Repeat([]byte("z"), end-start+1)
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, objectSize))
			w.Header().Set("Content-Length", strconv.Itoa(len(chunk)))
			w.WriteHeader(http.StatusPartialContent)
			w.Write(chunk)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	originalBucket := bucket
	originalRoot := root
	origPartSize := downloadPartSize
	origConcurrency := downloadConcurrency
	defer func() {
		bucket = originalBucket
		root = originalRoot
		downloadPartSize = origPartSize
		downloadConcurrency = origConcurrency
	}()
	bucket = "test-bucket"
	root = ""
	downloadPartSize = 8 * 1024 * 1024
	downloadConcurrency = 2

	s3Client := s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(server.URL),
		UsePathStyle: true,
		Credentials: aws.CredentialsProviderFunc(func(_ context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		}),
		Retryer: func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) { o.MaxAttempts = 1 })
		}(),
	})

	bh := &S3BackupHandle{
		s3Client: s3Client,
		bs: &S3BackupStorage{
			params: backupstorage.NoParams(),
			s3SSE:  S3ServerSideEncryption{},
		},
		dir:      "testdir",
		name:     "testbackup",
		readOnly: true,
	}

	rc, err := bh.ReadFile(t.Context(), "testfile")
	require.NoError(t, err)

	// Read until we hit the error
	buf := make([]byte, 64*1024)
	var sawError bool
	for {
		_, readErr := rc.Read(buf)
		if readErr != nil {
			if readErr != io.EOF {
				sawError = true
			}
			break
		}
	}
	assert.True(t, sawError, "expected a non-EOF error to surface from failed range GET")
	rc.Close()
}

func TestReadFileCloseAfterPartialRead(t *testing.T) {
	// Verifies that Close() works correctly after only partially reading an object.
	const objectSize = 24 * 1024 * 1024 // 24MiB
	testData := bytes.Repeat([]byte("p"), objectSize)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			w.Header().Set("Content-Length", strconv.Itoa(len(testData)))
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method == "GET" {
			rangeHdr := r.Header.Get("Range")
			if rangeHdr == "" {
				w.Header().Set("Content-Length", strconv.Itoa(len(testData)))
				w.WriteHeader(http.StatusOK)
				w.Write(testData)
				return
			}
			var start, end int
			fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)
			if end >= len(testData) {
				end = len(testData) - 1
			}
			chunk := testData[start : end+1]
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(testData)))
			w.Header().Set("Content-Length", strconv.Itoa(len(chunk)))
			w.WriteHeader(http.StatusPartialContent)
			w.Write(chunk)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	originalBucket := bucket
	originalRoot := root
	origPartSize := downloadPartSize
	origConcurrency := downloadConcurrency
	defer func() {
		bucket = originalBucket
		root = originalRoot
		downloadPartSize = origPartSize
		downloadConcurrency = origConcurrency
	}()
	bucket = "test-bucket"
	root = ""
	downloadPartSize = 8 * 1024 * 1024
	downloadConcurrency = 3

	s3Client := s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(server.URL),
		UsePathStyle: true,
		Credentials: aws.CredentialsProviderFunc(func(_ context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		}),
		Retryer: func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) { o.MaxAttempts = 1 })
		}(),
	})

	bh := &S3BackupHandle{
		s3Client: s3Client,
		bs: &S3BackupStorage{
			params: backupstorage.NoParams(),
			s3SSE:  S3ServerSideEncryption{},
		},
		dir:      "testdir",
		name:     "testbackup",
		readOnly: true,
	}

	rc, err := bh.ReadFile(t.Context(), "testfile")
	require.NoError(t, err)

	// Read only 1 MiB of the 24 MiB object
	buf := make([]byte, 1024*1024)
	n, err := io.ReadFull(rc, buf)
	require.NoError(t, err)
	require.Equal(t, 1024*1024, n)

	// Close without reading the rest — must not panic or hang
	err = rc.Close()
	require.NoError(t, err)
}

func TestReadFileSubPartObject(t *testing.T) {
	// Tests reading an object smaller than one download part (e.g. MANIFEST).
	// The transfer manager may use a single GET or a simpler code path for these.
	testData := []byte(`{"files": ["file1.xbstream", "file2.xbstream"]}`)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			w.Header().Set("Content-Length", strconv.Itoa(len(testData)))
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method == "GET" {
			w.Header().Set("Content-Length", strconv.Itoa(len(testData)))
			w.WriteHeader(http.StatusOK)
			w.Write(testData)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	originalBucket := bucket
	originalRoot := root
	origPartSize := downloadPartSize
	origConcurrency := downloadConcurrency
	defer func() {
		bucket = originalBucket
		root = originalRoot
		downloadPartSize = origPartSize
		downloadConcurrency = origConcurrency
	}()
	bucket = "test-bucket"
	root = ""
	downloadPartSize = 8 * 1024 * 1024 // 8MiB — object is only ~47 bytes
	downloadConcurrency = 5

	s3Client := s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(server.URL),
		UsePathStyle: true,
		Credentials: aws.CredentialsProviderFunc(func(_ context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		}),
		Retryer: func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) { o.MaxAttempts = 1 })
		}(),
	})

	bh := &S3BackupHandle{
		s3Client: s3Client,
		bs: &S3BackupStorage{
			params: backupstorage.NoParams(),
			s3SSE:  S3ServerSideEncryption{},
		},
		dir:      "testdir",
		name:     "testbackup",
		readOnly: true,
	}

	rc, err := bh.ReadFile(t.Context(), "MANIFEST")
	require.NoError(t, err)

	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, testData, data)

	err = rc.Close()
	require.NoError(t, err)
}

func TestReadFileOnWriteHandle(t *testing.T) {
	bh := &S3BackupHandle{
		readOnly: false,
	}

	_, err := bh.ReadFile(t.Context(), "testfile")
	require.Error(t, err, "ReadFile() should error on write handle")
	require.Contains(t, err.Error(), "cannot be called on read-write backup")
}

func TestAddFileOnReadOnlyHandle(t *testing.T) {
	bh := &S3BackupHandle{
		readOnly: true,
	}

	_, err := bh.AddFile(t.Context(), "testfile", 100)
	require.Error(t, err, "AddFile() should error on read-only handle")
	require.Contains(t, err.Error(), "cannot be called on read-only backup")
}

func TestEndBackupOnReadOnlyHandle(t *testing.T) {
	bh := &S3BackupHandle{
		readOnly: true,
	}

	err := bh.EndBackup(t.Context())
	require.Error(t, err, "EndBackup() should error on read-only handle")
	require.Contains(t, err.Error(), "cannot be called on read-only backup")
}

func TestAbortBackupOnReadOnlyHandle(t *testing.T) {
	bh := &S3BackupHandle{
		readOnly: true,
	}

	err := bh.AbortBackup(t.Context())
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
	wc, err := bh.AddFile(t.Context(), "testfile", 100)
	require.NoError(t, err)
	n, err := wc.Write([]byte("test data"))
	require.NoError(t, err)
	require.Equal(t, len("test data"), n)
	err = wc.Close()
	require.NoError(t, err)

	// End the backup
	err = bh.EndBackup(t.Context())
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
	wc, err := bh.AddFile(t.Context(), "testfile", 100)
	require.NoError(t, err)
	n, err := wc.Write([]byte("test data"))
	require.NoError(t, err)
	require.Equal(t, len("test data"), n)
	err = wc.Close()
	require.NoError(t, err)

	// End the backup - should return the error
	err = bh.EndBackup(t.Context())
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
	setSSEForTest(t, "")

	sseData := S3ServerSideEncryption{}
	err := sseData.init()
	require.NoError(t, err, "init() expected to succeed")

	assert.Empty(t, sseData.awsAlg, "awsAlg expected to be empty")
	assert.Nil(t, sseData.customerAlg, "customerAlg expected to be nil")
	assert.Nil(t, sseData.customerKey, "customerKey expected to be nil")
	assert.Nil(t, sseData.customerMd5, "customerMd5 expected to be nil")

	sseData.reset()
}

func TestSSEAws(t *testing.T) {
	setSSEForTest(t, "aws:kms")

	sseData := S3ServerSideEncryption{}
	err := sseData.init()
	require.NoError(t, err, "init() expected to succeed")

	assert.Equal(t, types.ServerSideEncryption("aws:kms"), sseData.awsAlg, "awsAlg expected to be aws:kms")
	assert.Nil(t, sseData.customerAlg, "customerAlg expected to be nil")
	assert.Nil(t, sseData.customerKey, "customerKey expected to be nil")
	assert.Nil(t, sseData.customerMd5, "customerMd5 expected to be nil")

	sseData.reset()

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

	setSSEForTest(t, sseCustomerPrefix+tempFile.Name())

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

	setSSEForTest(t, sseCustomerPrefix+tempFile.Name())

	sseData := S3ServerSideEncryption{}
	err = sseData.init()
	require.NoError(t, err, "init() expected to succeed")

	assert.Empty(t, sseData.awsAlg, "awsAlg expected to be empty")
	assert.Equal(t, aws.String("AES256"), sseData.customerAlg, "customerAlg expected to be AES256")
	assert.Equal(t, aws.String(string(randomKey)), sseData.customerKey, "customerKey expected to be equal to the generated randomKey")
	md5Hash := md5.Sum(randomKey)
	assert.Equal(t, aws.String(base64.StdEncoding.EncodeToString(md5Hash[:])), sseData.customerMd5, "customerMd5 expected to be equal to the customerMd5 hash of the generated randomKey")

	sseData.reset()

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

	setSSEForTest(t, sseCustomerPrefix+tempFile.Name())

	sseData := S3ServerSideEncryption{}
	err = sseData.init()
	require.NoError(t, err, "init() expected to succeed")

	assert.Empty(t, sseData.awsAlg, "awsAlg expected to be empty")
	assert.Equal(t, aws.String("AES256"), sseData.customerAlg, "customerAlg expected to be AES256")
	assert.Equal(t, aws.String(string(randomKey)), sseData.customerKey, "customerKey expected to be equal to the generated randomKey")
	md5Hash := md5.Sum(randomKey)
	assert.Equal(t, aws.String(base64.StdEncoding.EncodeToString(md5Hash[:])), sseData.customerMd5, "customerMd5 expected to be equal to the customerMd5 hash of the generated randomKey")

	sseData.reset()

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
	require.NoError(t, bs.s3SSE.init())

	bh := &S3BackupHandle{
		s3Client: client,
		bs:       bs,
		dir:      "testdir",
		name:     "testbackup",
		readOnly: false,
	}

	// Add a file
	wc, err := bh.AddFile(t.Context(), "testfile", 100)
	require.NoError(t, err)
	wc.Write([]byte("test data"))
	wc.Close()
	bh.waitGroup.Wait()

	// Abort the backup
	err = bh.AbortBackup(t.Context())
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
	wc, err := bh.AddFile(t.Context(), "largefile", largeFileSize)
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
	wc, err := bh.AddFile(t.Context(), "smallfile", fileSize)
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

	_, err := bh.AddFile(t.Context(), "testfile", 100)
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

	wc, err := bh.AddFile(t.Context(), "encrypted-file", 100)
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
	_, err := bh.ReadFile(t.Context(), "nonexistent")
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
	require.NoError(t, bs.s3SSE.init())

	// Update mock to return proper list response
	mockServer.SetHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			mockServer.serveDefault(w, r)
		}
	}))

	backups, err := bs.ListBackups(t.Context(), "testdir")
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
	require.NoError(t, bs.s3SSE.init())

	mockServer.SetHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			mockServer.serveDefault(w, r)
		}
	}))

	backups, err := bs.ListBackups(t.Context(), "/")
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
	require.NoError(t, bs.s3SSE.init())

	requestCount := 0
	mockServer.SetHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			mockServer.serveDefault(w, r)
		}
	}))

	backups, err := bs.ListBackups(t.Context(), "testdir")
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
	require.NoError(t, bs.s3SSE.init())

	bh, err := bs.StartBackup(t.Context(), "testdir", "newbackup")
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
	require.NoError(t, bs.s3SSE.init())

	mockServer.SetHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			mockServer.serveDefault(w, r)
		}
	}))

	err := bs.RemoveBackup(t.Context(), "testdir", "backup1")
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
	require.NoError(t, bs.s3SSE.init())

	mockServer.SetHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			mockServer.serveDefault(w, r)
		}
	}))

	err := bs.RemoveBackup(t.Context(), "testdir", "backup1")
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
	require.NoError(t, bs.s3SSE.init())

	requestCount := 0
	mockServer.SetHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			mockServer.serveDefault(w, r)
		}
	}))

	err := bs.RemoveBackup(t.Context(), "testdir", "backup1")
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
	require.NoError(t, bs.s3SSE.init())

	_, err := bs.ListBackups(t.Context(), "testdir")
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

	_, err := bs.StartBackup(t.Context(), "testdir", "newbackup")
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

	err := bs.RemoveBackup(t.Context(), "testdir", "backup1")
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

	mockServer.SetHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			mockServer.serveDefault(w, r)
		}
	}))

	err := bs.RemoveBackup(t.Context(), "testdir", "backup1")
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
	resolvedEndpoint, err := resolver.ResolveEndpoint(t.Context(), params)
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
	ctx := t.Context()
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
	require.NoError(t, bs.s3SSE.init())

	// Start a new backup
	bh, err := bs.StartBackup(t.Context(), "testdir", "full-backup")
	require.NoError(t, err)
	require.False(t, bh.(*S3BackupHandle).readOnly)

	// Add multiple files
	testData1 := []byte("file 1 contents")
	testData2 := []byte("file 2 contents with more data")

	wc1, err := bh.AddFile(t.Context(), "file1.dat", int64(len(testData1)))
	require.NoError(t, err)
	_, err = wc1.Write(testData1)
	require.NoError(t, err)
	err = wc1.Close()
	require.NoError(t, err)

	wc2, err := bh.AddFile(t.Context(), "file2.dat", int64(len(testData2)))
	require.NoError(t, err)
	_, err = wc2.Write(testData2)
	require.NoError(t, err)
	err = wc2.Close()
	require.NoError(t, err)

	// End the backup
	err = bh.EndBackup(t.Context())
	require.NoError(t, err)

	// Close and reopen storage
	err = bs.Close()
	require.NoError(t, err)

	bs2 := &S3BackupStorage{
		_client: client,
		params:  backupstorage.Params{Logger: logutil.NewMemoryLogger(), Stats: stats.NewFakeStats()},
	}
	require.NoError(t, bs2.s3SSE.init())

	// Set up mock to return proper list for ListBackups
	mockServer.SetHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		} else {
			mockServer.serveDefault(w, r)
		}
	}))

	// List backups
	backups, err := bs2.ListBackups(t.Context(), "testdir")
	require.NoError(t, err)
	require.Len(t, backups, 1)
	require.Equal(t, "full-backup", backups[0].Name())

	// Read files back
	readBh := backups[0]
	require.True(t, readBh.(*S3BackupHandle).readOnly)

	rc1, err := readBh.ReadFile(t.Context(), "file1.dat")
	require.NoError(t, err)
	data1, err := io.ReadAll(rc1)
	require.NoError(t, err)
	require.Equal(t, testData1, data1)
	rc1.Close()

	rc2, err := readBh.ReadFile(t.Context(), "file2.dat")
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

	bh, err := bs.StartBackup(t.Context(), "encrypted", "backup1")
	require.NoError(t, err)

	wc, err := bh.AddFile(t.Context(), "secret.txt", 100)
	require.NoError(t, err)

	_, err = wc.Write([]byte("secret data"))
	require.NoError(t, err)

	err = wc.Close()
	require.NoError(t, err)

	err = bh.EndBackup(t.Context())
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

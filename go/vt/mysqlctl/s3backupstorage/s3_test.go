package s3backupstorage

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/logutil"
	stats "vitess.io/vitess/go/vt/mysqlctl/backupstats"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
)

type s3FakeClient struct {
	*s3.Client
	err   error
	delay time.Duration
}

type fakeClientDo struct {
	delay time.Duration
}

func (fcd fakeClientDo) Do(request *http.Request) (*http.Response, error) {
	if fcd.delay > 0 {
		time.Sleep(fcd.delay)
	}
	return nil, nil
}

func (sfc *s3FakeClient) PutObject(ctx context.Context, in *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	var o s3.Options
	for _, fn := range optFns {
		fn(&o)
	}

	stack := middleware.NewStack("PutObject", smithyhttp.NewStackRequest)
	for _, apiOption := range o.APIOptions {
		_ = apiOption(stack)
	}

	handler := middleware.DecorateHandler(smithyhttp.NewClientHandler(&fakeClientDo{delay: sfc.delay}), stack)
	_, _, err := handler.Handle(ctx, in)
	if err != nil {
		return nil, err
	}

	if sfc.err != nil {
		return nil, sfc.err
	}

	return &s3.PutObjectOutput{
		ETag: aws.String("fake-etag"),
	}, nil
}

func TestAddFileError(t *testing.T) {
	bh := &S3BackupHandle{
		client: &s3FakeClient{err: errors.New("some error")},
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
	require.NoErrorf(t, err, "AddFile() expected no error, got %s", err)
	assert.NotNil(t, wc, "AddFile() expected non-nil WriteCloser")

	n, err := wc.Write([]byte("here are some bytes"))
	require.NoErrorf(t, err, "TestAddFile() could not write to uploader, got %d bytes written, err %s", n, err)

	err = wc.Close()
	require.NoErrorf(t, err, "TestAddFile() could not close writer, got %s", err)

	bh.waitGroup.Wait() // wait for the goroutine to finish, at which point it should have recorded an error

	require.True(t, bh.HasErrors(), "AddFile() expected bh to record async error but did not")
}

func TestAddFileStats(t *testing.T) {
	fakeStats := stats.NewFakeStats()

	delay := 10 * time.Millisecond

	bh := &S3BackupHandle{
		client: &s3FakeClient{delay: delay},
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

	for i := 0; i < 4; i++ {
		wc, err := bh.AddFile(context.Background(), fmt.Sprintf("somefile-%d", i), 100000)
		require.NoErrorf(t, err, "AddFile() expected no error, got %s", err)
		assert.NotNil(t, wc, "AddFile() expected non-nil WriteCloser")

		n, err := wc.Write([]byte("here are some bytes"))
		require.NoErrorf(t, err, "TestAddFile() could not write to uploader, got %d bytes written, err %s", n, err)

		err = wc.Close()
		require.NoErrorf(t, err, "TestAddFile() could not close writer, got %s", err)
	}

	bh.waitGroup.Wait() // wait for the goroutine to finish, at which point it should have recorded an error

	require.Equal(t, bh.HasErrors(), false, "AddFile() expected bh not to record async errors but did")

	require.Len(t, fakeStats.ScopeCalls, 4)
	scopedStats := fakeStats.ScopeReturns[0]
	require.Len(t, scopedStats.ScopeV, 1)
	require.Equal(t, scopedStats.ScopeV[stats.ScopeOperation], "AWS:Request:Send")
	require.Len(t, scopedStats.TimedIncrementCalls, 1)
	require.GreaterOrEqual(t, scopedStats.TimedIncrementCalls[0], delay)
	require.Len(t, scopedStats.TimedIncrementBytesCalls, 0)
}

func TestAddFileErrorStats(t *testing.T) {
	fakeStats := stats.NewFakeStats()

	delay := 10 * time.Millisecond

	bh := &S3BackupHandle{
		client: &s3FakeClient{
			delay: delay,
			err:   errors.New("some error"),
		},
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
	require.NoErrorf(t, err, "AddFile() expected no error, got %s", err)
	assert.NotNil(t, wc, "AddFile() expected non-nil WriteCloser")

	n, err := wc.Write([]byte("here are some bytes"))
	require.NoErrorf(t, err, "TestAddFile() could not write to uploader, got %d bytes written, err %s", n, err)

	err = wc.Close()
	require.NoErrorf(t, err, "TestAddFile() could not close writer, got %s", err)

	bh.waitGroup.Wait() // wait for the goroutine to finish, at which point it should have recorded an error

	require.True(t, bh.HasErrors(), "AddFile() expected bh not to record async errors but did")

	require.Len(t, fakeStats.ScopeCalls, 1)
	scopedStats := fakeStats.ScopeReturns[0]
	require.Len(t, scopedStats.ScopeV, 1)
	require.Equal(t, scopedStats.ScopeV[stats.ScopeOperation], "AWS:Request:Send")
	require.Len(t, scopedStats.TimedIncrementCalls, 1)
	require.GreaterOrEqual(t, scopedStats.TimedIncrementCalls[0], delay)
	require.Len(t, scopedStats.TimedIncrementBytesCalls, 0)
}

func TestNoSSE(t *testing.T) {
	sseData := S3ServerSideEncryption{}
	err := sseData.init()
	require.NoErrorf(t, err, "init() expected to succeed")

	assert.Empty(t, sseData.awsAlg, "awsAlg expected to be empty")
	assert.Nil(t, sseData.customerAlg, "customerAlg expected to be nil")
	assert.Nil(t, sseData.customerKey, "customerKey expected to be nil")
	assert.Nil(t, sseData.customerMd5, "customerMd5 expected to be nil")

	sseData.reset()
	require.NoErrorf(t, err, "reset() expected to succeed")
}

func TestSSEAws(t *testing.T) {
	sse = "aws:kms"
	sseData := S3ServerSideEncryption{}
	err := sseData.init()
	require.NoErrorf(t, err, "init() expected to succeed")

	assert.Equal(t, types.ServerSideEncryption("aws:kms"), sseData.awsAlg, "awsAlg expected to be aws:kms")
	assert.Nil(t, sseData.customerAlg, "customerAlg expected to be nil")
	assert.Nil(t, sseData.customerKey, "customerKey expected to be nil")
	assert.Nil(t, sseData.customerMd5, "customerMd5 expected to be nil")

	sseData.reset()
	require.NoErrorf(t, err, "reset() expected to succeed")

	assert.Empty(t, sseData.awsAlg, "awsAlg expected to be empty")
	assert.Nil(t, sseData.customerAlg, "customerAlg expected to be nil")
	assert.Nil(t, sseData.customerKey, "customerKey expected to be nil")
	assert.Nil(t, sseData.customerMd5, "customerMd5 expected to be nil")
}

func TestSSECustomerFileNotFound(t *testing.T) {
	tempFile, err := os.CreateTemp("", "filename")
	require.NoErrorf(t, err, "TempFile() expected to succeed")
	defer os.Remove(tempFile.Name())

	err = tempFile.Close()
	require.NoErrorf(t, err, "Close() expected to succeed")

	err = os.Remove(tempFile.Name())
	require.NoErrorf(t, err, "Remove() expected to succeed")

	sse = sseCustomerPrefix + tempFile.Name()
	sseData := S3ServerSideEncryption{}
	err = sseData.init()
	require.Errorf(t, err, "init() expected to fail")
}

func TestSSECustomerFileBinaryKey(t *testing.T) {
	tempFile, err := os.CreateTemp("", "filename")
	require.NoErrorf(t, err, "TempFile() expected to succeed")
	defer os.Remove(tempFile.Name())

	randomKey := make([]byte, 32)
	_, err = rand.Read(randomKey)
	require.NoErrorf(t, err, "Read() expected to succeed")
	_, err = tempFile.Write(randomKey)
	require.NoErrorf(t, err, "Write() expected to succeed")
	err = tempFile.Close()
	require.NoErrorf(t, err, "Close() expected to succeed")

	sse = sseCustomerPrefix + tempFile.Name()
	sseData := S3ServerSideEncryption{}
	err = sseData.init()
	require.NoErrorf(t, err, "init() expected to succeed")

	assert.Empty(t, sseData.awsAlg, "awsAlg expected to be empty")
	assert.Equal(t, aws.String("AES256"), sseData.customerAlg, "customerAlg expected to be AES256")
	assert.Equal(t, aws.String(string(randomKey)), sseData.customerKey, "customerKey expected to be equal to the generated randomKey")
	md5Hash := md5.Sum(randomKey)
	assert.Equal(t, aws.String(base64.StdEncoding.EncodeToString(md5Hash[:])), sseData.customerMd5, "customerMd5 expected to be equal to the customerMd5 hash of the generated randomKey")

	sseData.reset()
	require.NoErrorf(t, err, "reset() expected to succeed")

	assert.Empty(t, sseData.awsAlg, "awsAlg expected to be empty")
	assert.Nil(t, sseData.customerAlg, "customerAlg expected to be nil")
	assert.Nil(t, sseData.customerKey, "customerKey expected to be nil")
	assert.Nil(t, sseData.customerMd5, "customerMd5 expected to be nil")
}

func TestSSECustomerFileBase64Key(t *testing.T) {
	tempFile, err := os.CreateTemp("", "filename")
	require.NoErrorf(t, err, "TempFile() expected to succeed")
	defer os.Remove(tempFile.Name())

	randomKey := make([]byte, 32)
	_, err = rand.Read(randomKey)
	require.NoErrorf(t, err, "Read() expected to succeed")

	base64Key := base64.StdEncoding.EncodeToString(randomKey[:])
	_, err = tempFile.WriteString(base64Key)
	require.NoErrorf(t, err, "WriteString() expected to succeed")
	err = tempFile.Close()
	require.NoErrorf(t, err, "Close() expected to succeed")

	sse = sseCustomerPrefix + tempFile.Name()
	sseData := S3ServerSideEncryption{}
	err = sseData.init()
	require.NoErrorf(t, err, "init() expected to succeed")

	assert.Empty(t, sseData.awsAlg, "awsAlg expected to be empty")
	assert.Equal(t, aws.String("AES256"), sseData.customerAlg, "customerAlg expected to be AES256")
	assert.Equal(t, aws.String(string(randomKey)), sseData.customerKey, "customerKey expected to be equal to the generated randomKey")
	md5Hash := md5.Sum(randomKey)
	assert.Equal(t, aws.String(base64.StdEncoding.EncodeToString(md5Hash[:])), sseData.customerMd5, "customerMd5 expected to be equal to the customerMd5 hash of the generated randomKey")

	sseData.reset()
	require.NoErrorf(t, err, "reset() expected to succeed")

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

package s3backupstorage

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type s3ErrorClient struct{ s3iface.S3API }

func (s3errclient *s3ErrorClient) PutObjectRequest(in *s3.PutObjectInput) (*request.Request, *s3.PutObjectOutput) {
	req := request.Request{
		HTTPRequest: &http.Request{},          // without this we segfault \_(ツ)_/¯ (see https://github.com/aws/aws-sdk-go/blob/v1.28.8/aws/request/request_context.go#L13)
		Error:       errors.New("some error"), // this forces req.Send() (which is called by the uploader) to always return non-nil error
	}

	return &req, &s3.PutObjectOutput{}
}

func TestAddFileError(t *testing.T) {
	bh := &S3BackupHandle{client: &s3ErrorClient{}, bs: &S3BackupStorage{}, readOnly: false}

	wc, err := bh.AddFile(aws.BackgroundContext(), "somefile", 100000)
	require.NoErrorf(t, err, "AddFile() expected no error, got %s", err)
	assert.NotNil(t, wc, "AddFile() expected non-nil WriteCloser")

	n, err := wc.Write([]byte("here are some bytes"))
	require.NoErrorf(t, err, "TestAddFile() could not write to uploader, got %d bytes written, err %s", n, err)

	err = wc.Close()
	require.NoErrorf(t, err, "TestAddFile() could not close writer, got %s", err)

	bh.waitGroup.Wait() // wait for the goroutine to finish, at which point it should have recorded an error

	require.Equal(t, bh.HasErrors(), true, "AddFile() expected bh to record async error but did not")
}

func TestNoSSECustomer(t *testing.T) {
	sse := S3SSECustomer{}
	err := sse.Open()
	require.NoErrorf(t, err, "Open() expected to succeed")

	assert.Nil(t, sse.alg, "S3SSECustomer.alg expected to be nil")
	assert.Nil(t, sse.key, "S3SSECustomer.key expected to be nil")
	assert.Nil(t, sse.md5, "S3SSECustomer.md5 expected to be nil")

	sse.Close()
	require.NoErrorf(t, err, "Close() expected to succeed")
}

func TestSSECustomerFileNotFound(t *testing.T) {
	tempFile, err := ioutil.TempFile("", "filename")
	require.NoErrorf(t, err, "TempFile() expected to succeed")
	defer os.Remove(tempFile.Name())

	err = tempFile.Close()
	require.NoErrorf(t, err, "Close() expected to succeed")

	err = os.Remove(tempFile.Name())
	require.NoErrorf(t, err, "Remove() expected to succeed")

	sseCustomerKeyFile = aws.String(tempFile.Name())
	sse := S3SSECustomer{}
	err = sse.Open()
	require.Errorf(t, err, "Open() expected to fail")
}

func TestSSECustomerFileBinaryKey(t *testing.T) {
	tempFile, err := ioutil.TempFile("", "filename")
	require.NoErrorf(t, err, "TempFile() expected to succeed")
	defer os.Remove(tempFile.Name())

	randomKey := make([]byte, 32)
	_, err = rand.Read(randomKey)
	require.NoErrorf(t, err, "Read() expected to succeed")
	_, err = tempFile.Write(randomKey)
	require.NoErrorf(t, err, "Write() expected to succeed")
	err = tempFile.Close()
	require.NoErrorf(t, err, "Close() expected to succeed")

	sseCustomerKeyFile = aws.String(tempFile.Name())
	sse := S3SSECustomer{}
	err = sse.Open()
	require.NoErrorf(t, err, "Open() expected to succeed")

	assert.Equal(t, aws.String("AES256"), sse.alg, "S3SSECustomer.alg expected to be AES256")
	assert.Equal(t, aws.String(string(randomKey)), sse.key, "S3SSECustomer.key expected to be equal to the generated randomKey")
	md5Hash := md5.Sum(randomKey)
	assert.Equal(t, aws.String(base64.StdEncoding.EncodeToString(md5Hash[:])), sse.md5, "S3SSECustomer.md5 expected to be equal to the md5 hash of the generated randomKey")

	sse.Close()
	require.NoErrorf(t, err, "Close() expected to succeed")

	assert.Nil(t, sse.alg, "S3SSECustomer.alg expected to be nil")
	assert.Nil(t, sse.key, "S3SSECustomer.key expected to be nil")
	assert.Nil(t, sse.md5, "S3SSECustomer.md5 expected to be nil")
}

func TestSSECustomerFileBase64Key(t *testing.T) {
	tempFile, err := ioutil.TempFile("", "filename")
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

	sseCustomerKeyFile = aws.String(tempFile.Name())
	sse := S3SSECustomer{}
	err = sse.Open()
	require.NoErrorf(t, err, "Open() expected to succeed")

	assert.Equal(t, aws.String("AES256"), sse.alg, "S3SSECustomer.alg expected to be AES256")
	assert.Equal(t, aws.String(string(randomKey)), sse.key, "S3SSECustomer.key expected to be equal to the generated randomKey")
	md5Hash := md5.Sum(randomKey)
	assert.Equal(t, aws.String(base64.StdEncoding.EncodeToString(md5Hash[:])), sse.md5, "S3SSECustomer.md5 expected to be equal to the md5 hash of the generated randomKey")

	sse.Close()
	require.NoErrorf(t, err, "Close() expected to succeed")

	assert.Nil(t, sse.alg, "S3SSECustomer.alg expected to be nil")
	assert.Nil(t, sse.key, "S3SSECustomer.key expected to be nil")
	assert.Nil(t, sse.md5, "S3SSECustomer.md5 expected to be nil")
}

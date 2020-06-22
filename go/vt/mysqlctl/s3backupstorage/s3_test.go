package s3backupstorage

import (
	"errors"
	"net/http"
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
	bh := &S3BackupHandle{client: &s3ErrorClient{}, readOnly: false}

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

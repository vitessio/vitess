package s3backupstorage

import (
	"errors"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
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

	if err != nil {
		t.Errorf("AddFile() expected no error, got %s", err)
		return
	}

	if wc == nil {
		t.Errorf("AddFile() expected non-nil WriteCloser")
		return
	}

	n, err := wc.Write([]byte("here are some bytes"))
	if err != nil {
		t.Errorf("TestAddFile() could not write to uploader, got %d bytes written, err %s", n, err)
		return
	}

	if err := wc.Close(); err != nil {
		t.Errorf("TestAddFile() could not close writer, got %s", err)
		return
	}

	bh.waitGroup.Wait() // wait for the goroutine to finish, at which point it should have recorded an error

	if !bh.HasErrors() {
		t.Errorf("AddFile() expected bh to record async error but did not")
	}
}

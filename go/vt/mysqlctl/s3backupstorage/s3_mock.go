/*
Copyright 2024 The Vitess Authors.

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
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/backupstats"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
)

type FakeS3BackupHandle struct {
	*S3BackupHandle

	AddFileReturnF func(s3 *S3BackupHandle, ctx context.Context, filename string, filesize int64) (io.WriteCloser, error)
}

type FakeConfig struct {
	Region    string
	Endpoint  string
	Bucket    string
	ForcePath bool
}

func InitFlag(cfg FakeConfig) {
	region = cfg.Region
	endpoint = cfg.Endpoint
	bucket = cfg.Bucket
	forcePath = cfg.ForcePath
}

func NewFakeS3BackupHandle(ctx context.Context, dir, name string, logger logutil.Logger, stats backupstats.Stats) (*FakeS3BackupHandle, error) {
	s := newS3BackupStorage()
	bs := s.WithParams(backupstorage.Params{
		Logger: logger,
		Stats:  stats,
	})
	bh, err := bs.StartBackup(ctx, dir, name)
	if err != nil {
		return nil, err
	}
	return &FakeS3BackupHandle{
		S3BackupHandle: bh.(*S3BackupHandle),
	}, nil
}

func (fbh *FakeS3BackupHandle) Directory() string {
	return fbh.S3BackupHandle.Directory()
}

func (fbh *FakeS3BackupHandle) Name() string {
	return fbh.S3BackupHandle.Name()
}

func (fbh *FakeS3BackupHandle) AddFile(ctx context.Context, filename string, filesize int64) (io.WriteCloser, error) {
	if fbh.AddFileReturnF != nil {
		return fbh.AddFileReturnF(fbh.S3BackupHandle, ctx, filename, filesize)
	}
	return fbh.S3BackupHandle.AddFile(ctx, filename, filesize)
}

func (fbh *FakeS3BackupHandle) EndBackup(ctx context.Context) error {
	return fbh.S3BackupHandle.EndBackup(ctx)
}

func (fbh *FakeS3BackupHandle) AbortBackup(ctx context.Context) error {
	return fbh.S3BackupHandle.AbortBackup(ctx)
}

func (fbh *FakeS3BackupHandle) ReadFile(ctx context.Context, filename string) (io.ReadCloser, error) {
	return fbh.S3BackupHandle.ReadFile(ctx, filename)
}

func (fbh *FakeS3BackupHandle) RecordError(s string, err error) {
	fbh.S3BackupHandle.RecordError(s, err)
}

func (fbh *FakeS3BackupHandle) HasErrors() bool {
	return fbh.S3BackupHandle.HasErrors()
}

func (fbh *FakeS3BackupHandle) Error() error {
	return fbh.S3BackupHandle.Error()
}

func (fbh *FakeS3BackupHandle) GetFailedFiles() []string {
	return fbh.S3BackupHandle.GetFailedFiles()
}

func (fbh *FakeS3BackupHandle) ResetErrorForFile(s string) {
	fbh.S3BackupHandle.ResetErrorForFile(s)
}

var (
	firstReadMu  sync.Mutex
	firstReadMap = make(map[string]bool)
)

type failFirstRead struct {
	*io.PipeReader
}

func (fwr *failFirstRead) Read(p []byte) (n int, err error) {
	return 0, errors.New("failing first read")
}

func FailFirstWrite(s3bh *S3BackupHandle, ctx context.Context, filename string, filesize int64) (io.WriteCloser, error) {
	if s3bh.readOnly {
		return nil, fmt.Errorf("AddFile cannot be called on read-only backup")
	}

	partSizeBytes := calculateUploadPartSize(filesize)
	reader, writer := io.Pipe()
	r := io.Reader(reader)

	firstReadMu.Lock()
	defer firstReadMu.Unlock()
	if ok := firstReadMap[filename]; !ok {
		firstReadMap[filename] = true
		r = &failFirstRead{PipeReader: reader}
	}

	s3bh.waitGroup.Add(1)

	go func() {
		defer s3bh.waitGroup.Done()
		uploader := manager.NewUploader(s3bh.client, func(u *manager.Uploader) {
			u.PartSize = partSizeBytes
		})
		object := objName(s3bh.dir, s3bh.name, filename)
		sendStats := s3bh.bs.params.Stats.Scope(backupstats.Operation("AWS:Request:Send"))
		_, err := uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket:               &bucket,
			Key:                  &object,
			Body:                 r,
			ServerSideEncryption: s3bh.bs.s3SSE.awsAlg,
			SSECustomerAlgorithm: s3bh.bs.s3SSE.customerAlg,
			SSECustomerKey:       s3bh.bs.s3SSE.customerKey,
			SSECustomerKeyMD5:    s3bh.bs.s3SSE.customerMd5,
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
			reader.CloseWithError(err)
			s3bh.RecordError(filename, err)
		}
	}()

	return writer, nil
}

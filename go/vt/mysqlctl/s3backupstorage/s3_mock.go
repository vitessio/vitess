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
	"io"
	"sync"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/backupstats"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
)

type FakeS3BackupHandle struct {
	*S3BackupHandle

	AddFileReturnF  func(s3 *S3BackupHandle, ctx context.Context, filename string, filesize int64, firstAdd bool) (io.WriteCloser, error)
	ReadFileReturnF func(s3 *S3BackupHandle, ctx context.Context, filename string, firstRead bool) (io.ReadCloser, error)

	mu          sync.Mutex
	addPerFile  map[string]int
	readPerFile map[string]int
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
		addPerFile:     make(map[string]int),
		readPerFile:    make(map[string]int),
	}, nil
}

func NewFakeS3RestoreHandle(ctx context.Context, dir string, logger logutil.Logger, stats backupstats.Stats) (*FakeS3BackupHandle, error) {
	s := newS3BackupStorage()
	bs := s.WithParams(backupstorage.Params{
		Logger: logger,
		Stats:  stats,
	})
	bhs, err := bs.ListBackups(ctx, dir)
	if err != nil {
		return nil, err
	}
	return &FakeS3BackupHandle{
		S3BackupHandle: bhs[0].(*S3BackupHandle),
		addPerFile:     make(map[string]int),
		readPerFile:    make(map[string]int),
	}, nil
}

func (fbh *FakeS3BackupHandle) Directory() string {
	return fbh.S3BackupHandle.Directory()
}

func (fbh *FakeS3BackupHandle) Name() string {
	return fbh.S3BackupHandle.Name()
}

func (fbh *FakeS3BackupHandle) AddFile(ctx context.Context, filename string, filesize int64) (io.WriteCloser, error) {
	fbh.mu.Lock()
	defer func() {
		fbh.addPerFile[filename] += 1
		fbh.mu.Unlock()
	}()

	if fbh.AddFileReturnF != nil {
		return fbh.AddFileReturnF(fbh.S3BackupHandle, ctx, filename, filesize, fbh.addPerFile[filename] == 0)
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
	fbh.mu.Lock()
	defer func() {
		fbh.readPerFile[filename] += 1
		fbh.mu.Unlock()
	}()

	if fbh.ReadFileReturnF != nil {
		return fbh.ReadFileReturnF(fbh.S3BackupHandle, ctx, filename, fbh.readPerFile[filename] == 0)
	}
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

type failReadPipeReader struct {
	*io.PipeReader
}

func (fwr *failReadPipeReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("failing read")
}

func FailFirstWrite(s3bh *S3BackupHandle, ctx context.Context, filename string, filesize int64, firstAdd bool) (io.WriteCloser, error) {
	if s3bh.readOnly {
		return nil, errors.New("AddFile cannot be called on read-only backup")
	}

	partSizeBytes, err := calculateUploadPartSize(filesize)
	if err != nil {
		return nil, err
	}

	reader, writer := io.Pipe()
	r := io.Reader(reader)

	if firstAdd {
		r = &failReadPipeReader{PipeReader: reader}
	}

	s3bh.handleAddFile(ctx, filename, partSizeBytes, r, func(err error) {
		reader.CloseWithError(err)
	})
	return writer, nil
}

func FailAllWrites(s3bh *S3BackupHandle, ctx context.Context, filename string, filesize int64, _ bool) (io.WriteCloser, error) {
	if s3bh.readOnly {
		return nil, errors.New("AddFile cannot be called on read-only backup")
	}

	partSizeBytes, err := calculateUploadPartSize(filesize)
	if err != nil {
		return nil, err
	}

	reader, writer := io.Pipe()
	r := &failReadPipeReader{PipeReader: reader}

	s3bh.handleAddFile(ctx, filename, partSizeBytes, r, func(err error) {
		r.CloseWithError(err)
	})
	return writer, nil
}

type failRead struct{}

func (fr *failRead) Read(p []byte) (n int, err error) {
	return 0, errors.New("failing read")
}

func (fr *failRead) Close() error {
	return nil
}

func FailFirstRead(s3bh *S3BackupHandle, ctx context.Context, filename string, firstRead bool) (io.ReadCloser, error) {
	rc, err := s3bh.ReadFile(ctx, filename)
	if err != nil {
		return nil, err
	}
	if firstRead {
		return &failRead{}, nil
	}
	return rc, nil
}

// FailAllReadExpectManifest is used to fail every attempt at reading a file from S3.
// Only the MANIFEST file is allowed to be read, because otherwise we wouldn't even try to read the normal files.
func FailAllReadExpectManifest(s3bh *S3BackupHandle, ctx context.Context, filename string, _ bool) (io.ReadCloser, error) {
	const manifestFileName = "MANIFEST"
	if filename == manifestFileName {
		return s3bh.ReadFile(ctx, filename)
	}
	return &failRead{}, nil
}

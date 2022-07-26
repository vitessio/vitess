package gcsbackup

import (
	"context"
	"errors"
	"io"
	"path"
	"strconv"
	"sync/atomic"

	"cloud.google.com/go/storage"

	"vitess.io/vitess/go/vt/concurrency"
)

// ErrReadonly is returned when an attempt is made
// to write to a read-only backup.
var errReadonly = errors.New("cannot write to a read-only backup")

// Handle implements a backup handle.
type handle struct {
	bucket *storage.BucketHandle
	kms    *kms
	name   string
	id     string
	dir    string
	rw     bool
	size   int64 // total backup size
	concurrency.AllErrorRecorder
}

// NewHandle returns a new handle with root, dir and name.
func newHandle(bucket *storage.BucketHandle, kms *kms, id, dir, name string) *handle {
	return &handle{
		bucket: bucket,
		kms:    kms,
		id:     id,
		dir:    dir,
		name:   name,
		rw:     true,
		size:   0,
	}
}

// Readonly changes the h to readonly and returns h.
func (h *handle) readonly() *handle {
	h.rw = false
	return h
}

// Directory implementation.
func (h *handle) Directory() string {
	return h.dir
}

// Name implementation.
func (h *handle) Name() string {
	return h.name
}

// AddFile implementation.
func (h *handle) AddFile(ctx context.Context, filename string, size int64) (io.WriteCloser, error) {
	if !h.rw {
		return nil, errReadonly
	}

	atomic.AddInt64(&h.size, size)

	dst := h.object(filename).NewWriter(ctx)

	enc, err := newEncoder(ctx, h.kms, dst)
	if err != nil {
		return nil, err
	}

	return enc, nil
}

// EndBackup implementation.
func (h *handle) EndBackup(ctx context.Context) error {
	if !h.rw {
		return errReadonly
	}
	return h.uploadSizeFile(ctx)
}

// uploadSizeFile creates the SIZE file, writes the size to it, and then uploads it.
// we convert the size to a string instead of something more efficient to make
// debugging and working with the file easier
func (h *handle) uploadSizeFile(ctx context.Context) error {
	object := h.object("SIZE").NewWriter(ctx)
	size := atomic.LoadInt64(&h.size)
	sizeAsString := strconv.FormatInt(size, 10)

	if _, err := object.Write([]byte(sizeAsString)); err != nil {
		return err
	}

	if err := object.Close(); err != nil {
		return err
	}

	return nil
}

// AbortBackup implementation.
func (h *handle) AbortBackup(ctx context.Context) error {
	if !h.rw {
		return errReadonly
	}
	return nil
}

// ReadFile implementation.
func (h *handle) ReadFile(ctx context.Context, filename string) (io.ReadCloser, error) {
	src, err := h.object(filename).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	return newDecoder(ctx, h.kms, src)
}

// Object returns an object with filename.
func (h *handle) object(filename string) *storage.ObjectHandle {
	k := path.Join(h.id, h.dir, h.name, filename)
	o := h.bucket.Object(k)
	return o
}

// Copyright 2014 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"errors"
	"fmt"
	"io"
	"unicode/utf8"

	"golang.org/x/net/context"
	"google.golang.org/api/googleapi"
	raw "google.golang.org/api/storage/v1"
)

// A Writer writes a Cloud Storage object.
type Writer struct {
	// ObjectAttrs are optional attributes to set on the object. Any attributes
	// must be initialized before the first Write call. Nil or zero-valued
	// attributes are ignored.
	ObjectAttrs

	// ChunkSize controls the maximum number of bytes of the object that the
	// Writer will attempt to send to the server in a single request. Objects
	// smaller than the size will be sent in a single request, while larger
	// objects will be split over multiple requests. The size will be rounded up
	// to the nearest multiple of 256K. If zero, chunking will be disabled and
	// the object will be uploaded in a single request.
	//
	// ChunkSize will default to a reasonable value. Any custom configuration
	// must be done before the first Write call.
	ChunkSize int

	ctx context.Context
	o   *ObjectHandle

	opened bool
	pw     *io.PipeWriter

	donec chan struct{} // closed after err and obj are set.
	err   error
	obj   *ObjectAttrs
}

func (w *Writer) open() error {
	attrs := w.ObjectAttrs
	// Check the developer didn't change the object Name (this is unfortunate, but
	// we don't want to store an object under the wrong name).
	if attrs.Name != w.o.object {
		return fmt.Errorf("storage: Writer.Name %q does not match object name %q", attrs.Name, w.o.object)
	}
	if !utf8.ValidString(attrs.Name) {
		return fmt.Errorf("storage: object name %q is not valid UTF-8", attrs.Name)
	}
	pr, pw := io.Pipe()
	w.pw = pw
	w.opened = true

	if w.ChunkSize < 0 {
		return errors.New("storage: Writer.ChunkSize must non-negative")
	}
	mediaOpts := []googleapi.MediaOption{
		googleapi.ChunkSize(w.ChunkSize),
	}
	if c := attrs.ContentType; c != "" {
		mediaOpts = append(mediaOpts, googleapi.ContentType(c))
	}

	go func() {
		defer close(w.donec)

		call := w.o.c.raw.Objects.Insert(w.o.bucket, attrs.toRawObject(w.o.bucket)).
			Media(pr, mediaOpts...).
			Projection("full").
			Context(w.ctx)
		if err := setEncryptionHeaders(call.Header(), w.o.encryptionKey, false); err != nil {
			w.err = err
			pr.CloseWithError(w.err)
			return
		}
		var resp *raw.Object
		err := applyConds("NewWriter", w.o.gen, w.o.conds, call)
		if err == nil {
			err = runWithRetry(w.ctx, func() error { resp, err = call.Do(); return err })
		}
		if err != nil {
			w.err = err
			pr.CloseWithError(w.err)
			return
		}
		w.obj = newObject(resp)
	}()
	return nil
}

// Write appends to w. It implements the io.Writer interface.
func (w *Writer) Write(p []byte) (n int, err error) {
	if w.err != nil {
		return 0, w.err
	}
	if !w.opened {
		if err := w.open(); err != nil {
			return 0, err
		}
	}
	return w.pw.Write(p)
}

// Close completes the write operation and flushes any buffered data.
// If Close doesn't return an error, metadata about the written object
// can be retrieved by calling Object.
func (w *Writer) Close() error {
	if !w.opened {
		if err := w.open(); err != nil {
			return err
		}
	}
	if err := w.pw.Close(); err != nil {
		return err
	}
	<-w.donec
	return w.err
}

// CloseWithError aborts the write operation with the provided error.
// CloseWithError always returns nil.
func (w *Writer) CloseWithError(err error) error {
	if !w.opened {
		return nil
	}
	return w.pw.CloseWithError(err)
}

// Attrs returns metadata about a successfully-written object.
// It's only valid to call it after Close returns nil.
func (w *Writer) Attrs() *ObjectAttrs {
	return w.obj
}

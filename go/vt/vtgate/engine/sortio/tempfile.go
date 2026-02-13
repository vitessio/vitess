/*
Copyright 2025 The Vitess Authors.

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

package sortio

import (
	"bufio"
	"io"
	"os"
)

// TempFile manages a temporary file for writing and reading sorted runs.
// The file is unlinked immediately after creation so it is automatically
// cleaned up when closed (or if the process crashes).
type TempFile struct {
	f      *os.File
	writer *bufio.Writer
	offset int64
}

// NewTempFile creates a new temporary file in dir. If dir is empty, os.TempDir() is used.
// The file is immediately unlinked from the filesystem.
func NewTempFile(dir string, bufSize int) (*TempFile, error) {
	if dir == "" {
		dir = os.TempDir()
	}
	f, err := os.CreateTemp(dir, "vtgate-sort-*")
	if err != nil {
		return nil, err
	}
	// Unlink immediately - the fd keeps it alive
	os.Remove(f.Name())

	return &TempFile{
		f:      f,
		writer: bufio.NewWriterSize(f, bufSize),
	}, nil
}

// Write appends data to the temp file and returns the offset where it was written.
func (tf *TempFile) Write(data []byte) (offset int64, err error) {
	offset = tf.offset
	n, err := tf.writer.Write(data)
	tf.offset += int64(n)
	return offset, err
}

// Flush flushes the buffered writer to disk.
func (tf *TempFile) Flush() error {
	return tf.writer.Flush()
}

// NewReader creates a buffered reader for the given byte range in the file.
// The caller must call Flush() before creating readers.
func (tf *TempFile) NewReader(offset, length int64, bufSize int) *bufio.Reader {
	sr := io.NewSectionReader(tf.f, offset, length)
	return bufio.NewReaderSize(sr, bufSize)
}

// Reset flushes, truncates, and resets the write position for reuse.
func (tf *TempFile) Reset() error {
	if err := tf.writer.Flush(); err != nil {
		return err
	}
	tf.writer.Reset(tf.f)
	tf.offset = 0
	if err := tf.f.Truncate(0); err != nil {
		return err
	}
	_, err := tf.f.Seek(0, io.SeekStart)
	return err
}

// Close closes the underlying file.
func (tf *TempFile) Close() error {
	return tf.f.Close()
}

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

package mysqlctl

import (
	"io"
	"os"
	"unsafe"

	"vitess.io/vitess/go/vt/vterrors"
)

const (
	// directIOAlignment is the alignment of buffer addresses, lengths and file offsets used for
	// O_DIRECT writes. 4KiB satisfies the logical block size of the devices we write to.
	directIOAlignment = 4096

	// maxConsecutiveEmptyReads mirrors the bufio.Writer guard against readers that never make progress.
	maxConsecutiveEmptyReads = 100
)

// directIOWriter is a buffered writer for a file opened with O_DIRECT. w must write sequentially
// to f, starting at an aligned offset. Data is written to w from an aligned buffer, in multiples
// of directIOAlignment, except for a final remainder that does not fill a whole block: Flush
// writes it after disabling O_DIRECT on f.
type directIOWriter struct {
	f   *os.File
	w   io.Writer
	buf []byte
	n   int
}

// newDirectIOWriter returns a directIOWriter whose buffer holds at least size bytes, rounded up
// to a multiple of directIOAlignment.
func newDirectIOWriter(f *os.File, w io.Writer, size int) *directIOWriter {
	size = (max(size, directIOAlignment) + directIOAlignment - 1) / directIOAlignment * directIOAlignment
	return &directIOWriter{f: f, w: w, buf: alignedBuffer(size)}
}

// Write is part of the io.Writer interface.
func (d *directIOWriter) Write(p []byte) (int, error) {
	var written int
	for len(p) > 0 {
		if d.n == len(d.buf) {
			if err := d.flushFullBuffer(); err != nil {
				return written, err
			}
		}
		n := copy(d.buf[d.n:], p)
		d.n += n
		written += n
		p = p[n:]
	}
	return written, nil
}

// ReadFrom is part of the io.ReaderFrom interface. It reads straight into the aligned buffer,
// saving io.Copy an intermediate buffer and copy.
func (d *directIOWriter) ReadFrom(r io.Reader) (int64, error) {
	var total int64
	var emptyReads int
	for {
		if d.n == len(d.buf) {
			if err := d.flushFullBuffer(); err != nil {
				return total, err
			}
		}
		n, err := r.Read(d.buf[d.n:])
		d.n += n
		total += int64(n)
		if err == io.EOF {
			return total, nil
		}
		if err != nil {
			return total, err
		}
		if n > 0 {
			emptyReads = 0
		} else if emptyReads++; emptyReads >= maxConsecutiveEmptyReads {
			return total, io.ErrNoProgress
		}
	}
}

// Flush writes any buffered data. If that leaves a remainder smaller than directIOAlignment, it is
// written after disabling O_DIRECT on the file, so Flush is meant to be called after the last write.
func (d *directIOWriter) Flush() error {
	aligned := d.n / directIOAlignment * directIOAlignment
	if aligned > 0 {
		if err := d.write(d.buf[:aligned]); err != nil {
			return err
		}
	}
	if tail := d.buf[aligned:d.n]; len(tail) > 0 {
		if err := disableDirectIO(d.f); err != nil {
			return vterrors.Wrapf(err, "failed to disable O_DIRECT to write the last %d bytes", len(tail))
		}
		if err := d.write(tail); err != nil {
			return err
		}
	}
	d.n = 0
	return nil
}

func (d *directIOWriter) flushFullBuffer() error {
	if err := d.write(d.buf); err != nil {
		return err
	}
	d.n = 0
	return nil
}

func (d *directIOWriter) write(p []byte) error {
	n, err := d.w.Write(p)
	if err == nil && n < len(p) {
		err = io.ErrShortWrite
	}
	return err
}

// alignedBuffer returns a buffer of the given size whose address is a multiple of directIOAlignment.
func alignedBuffer(size int) []byte {
	buf := make([]byte, size+directIOAlignment)
	offset := int(-uintptr(unsafe.Pointer(&buf[0])) & (directIOAlignment - 1))
	return buf[offset : offset+size : offset+size]
}

// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cgzip

/*
#cgo pkg-config: zlib

#include "zlib.h"

// inflateInit2 is a macro, so using a wrapper function
int cgzipInflateInit(z_stream *strm) {
    strm->zalloc = Z_NULL;
    strm->zfree = Z_NULL;
    strm->opaque = Z_NULL;
    strm->avail_in = 0;
    strm->next_in = Z_NULL;
 return inflateInit2(strm,
                     16+15); // 16 makes it understand only gzip files
}
*/
import "C"

import (
	"fmt"
	"io"
	"unsafe"
)

// err starts out as nil
// we will call inflateEnd when we set err to a value:
// - whatever error is returned by the underlying reader
// - io.EOF if Close was called
type reader struct {
	r      io.Reader
	in     []byte
	strm   C.z_stream
	err    error
	skipIn bool
}

func NewReader(r io.Reader) (io.ReadCloser, error) {
	return NewReaderBuffer(r, DEFAULT_COMPRESSED_BUFFER_SIZE)
}

func NewReaderBuffer(r io.Reader, bufferSize int) (io.ReadCloser, error) {
	z := &reader{r: r, in: make([]byte, bufferSize)}
	result := C.cgzipInflateInit(&z.strm)
	if result != Z_OK {
		return nil, fmt.Errorf("cgzip: failed to initialize (%v): %v", result, C.GoString(z.strm.msg))
	}
	return z, nil
}

func (z *reader) Read(p []byte) (int, error) {
	if z.err != nil {
		return 0, z.err
	}

	if len(p) == 0 {
		return 0, nil
	}

	// read and deflate until the output buffer is full
	z.strm.next_out = (*C.Bytef)(unsafe.Pointer(&p[0]))
	z.strm.avail_out = (C.uInt)(len(p))

	for {
		// if we have no data to inflate, read more
		if !z.skipIn && z.strm.avail_in == 0 {
			var n int
			n, z.err = z.r.Read(z.in)
			if (z.err != nil && z.err != io.EOF) || (n == 0 && z.err == io.EOF) {
				C.inflateEnd(&z.strm)
				return 0, z.err
			}

			z.strm.next_in = (*C.Bytef)(unsafe.Pointer(&z.in[0]))
			z.strm.avail_in = (C.uInt)(n)
		} else {
			z.skipIn = false
		}

		// inflate some
		ret := C.inflate(&z.strm, C.Z_NO_FLUSH)
		switch ret {
		case Z_NEED_DICT:
			ret = Z_DATA_ERROR
			fallthrough
		case Z_DATA_ERROR, Z_MEM_ERROR:
			z.err = fmt.Errorf("cgzip: failed to inflate (%v): %v", ret, C.GoString(z.strm.msg))
			C.inflateEnd(&z.strm)
			return 0, z.err
		}

		// if we read something, we're good
		have := len(p) - int(z.strm.avail_out)
		if have > 0 {
			z.skipIn = ret == Z_OK && z.strm.avail_out == 0
			return have, z.err
		}
	}
	panic("Unreachable")
}

// Close closes the Reader. It does not close the underlying io.Reader.
func (z *reader) Close() error {
	if z.err != nil {
		if z.err != io.EOF {
			return z.err
		}
		return nil
	}
	C.inflateEnd(&z.strm)
	z.err = io.EOF
	return nil
}

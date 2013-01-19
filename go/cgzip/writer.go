// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cgzip

// See http://www.zlib.net/zlib_how.html for more information on this

/*
#cgo pkg-config: zlib

#include "zlib.h"

// deflateInit2 is a macro, so using a wrapper function
// using deflateInit2 instead of deflateInit to be able to specify gzip format
int cgzipDeflateInit(z_stream *strm, int level) {
    strm->zalloc = Z_NULL;
    strm->zfree = Z_NULL;
    strm->opaque = Z_NULL;
    return deflateInit2(strm, level, Z_DEFLATED,
                        16+15, // 16 makes it a gzip file, 15 is default
                        8, Z_DEFAULT_STRATEGY); // default values
}
*/
import "C"

import (
	"fmt"
	"io"
	"unsafe"
)

const (
	// Allowed flush values
	Z_NO_FLUSH      = 0
	Z_PARTIAL_FLUSH = 1
	Z_SYNC_FLUSH    = 2
	Z_FULL_FLUSH    = 3
	Z_FINISH        = 4
	Z_BLOCK         = 5
	Z_TREES         = 6

	// Return codes
	Z_OK            = 0
	Z_STREAM_END    = 1
	Z_NEED_DICT     = 2
	Z_ERRNO         = -1
	Z_STREAM_ERROR  = -2
	Z_DATA_ERROR    = -3
	Z_MEM_ERROR     = -4
	Z_BUF_ERROR     = -5
	Z_VERSION_ERROR = -6

	// compression levels
	Z_NO_COMPRESSION      = 0
	Z_BEST_SPEED          = 1
	Z_BEST_COMPRESSION    = 9
	Z_DEFAULT_COMPRESSION = -1

	// our default buffer size
	// most go io functions use 32KB as buffer size, so 32KB
	// works well here for compressed data buffer
	DEFAULT_COMPRESSED_BUFFER_SIZE = 32 * 1024
)

// err starts out as nil
// we will call deflateEnd when we set err to a value:
// - whatever error is returned by the underlying writer
// - io.EOF if Close was called
type Writer struct {
	w    io.Writer
	out  []byte
	strm C.z_stream
	err  error
}

func NewWriter(w io.Writer) *Writer {
	z, _ := NewWriterLevelBuffer(w, Z_DEFAULT_COMPRESSION, DEFAULT_COMPRESSED_BUFFER_SIZE)
	return z
}

func NewWriterLevel(w io.Writer, level int) (*Writer, error) {
	return NewWriterLevelBuffer(w, level, DEFAULT_COMPRESSED_BUFFER_SIZE)
}

func NewWriterLevelBuffer(w io.Writer, level, bufferSize int) (*Writer, error) {
	z := &Writer{w: w, out: make([]byte, bufferSize)}
	result := C.cgzipDeflateInit(&z.strm, (C.int)(level))
	if result != Z_OK {
		return nil, fmt.Errorf("cgzip: failed to initialize (%v): %v", result, C.GoString(z.strm.msg))
	}
	return z, nil
}

// this is the main function: it advances the write with either
// new data or something else to do, like a flush
func (z *Writer) write(p []byte, flush int) int {
	if len(p) == 0 {
		z.strm.next_in = (*C.Bytef)(unsafe.Pointer(nil))
		z.strm.avail_in = 0
	} else {
		z.strm.next_in = (*C.Bytef)(unsafe.Pointer(&p[0]))
		z.strm.avail_in = (C.uInt)(len(p))
	}
	// we loop until we don't get a full output buffer
	// each loop completely writes the output buffer to the underlying
	// writer
	for {
		// deflate one buffer
		z.strm.next_out = (*C.Bytef)(unsafe.Pointer(&z.out[0]))
		z.strm.avail_out = (C.uInt)(len(z.out))
		ret := C.deflate(&z.strm, (C.int)(flush))
		if ret == Z_STREAM_ERROR {
			// all the other error cases are normal,
			// and this should never happen
			panic(fmt.Errorf("cgzip: Unexpected error (1)"))
		}

		// write everything
		from := 0
		have := len(z.out) - int(z.strm.avail_out)
		for have > 0 {
			var n int
			n, z.err = z.w.Write(z.out[from:have])
			if z.err != nil {
				C.deflateEnd(&z.strm)
				return 0
			}
			from += n
			have -= n
		}

		// we stop trying if we get a partial response
		if z.strm.avail_out != 0 {
			break
		}
	}
	// the library guarantees this
	if z.strm.avail_in != 0 {
		panic(fmt.Errorf("cgzip: Unexpected error (2)"))
	}
	return len(p)
}

func (z *Writer) Write(p []byte) (n int, err error) {
	if z.err != nil {
		return 0, z.err
	}
	n = z.write(p, Z_NO_FLUSH)
	return n, z.err
}

func (z *Writer) Flush() error {
	if z.err != nil {
		return z.err
	}
	z.write(nil, Z_SYNC_FLUSH)
	return z.err
}

// Calling Close does not close the wrapped io.Writer originally
// passed to NewWriterX.
func (z *Writer) Close() error {
	if z.err != nil {
		return z.err
	}
	z.write(nil, Z_FINISH)
	if z.err != nil {
		return z.err
	}
	C.deflateEnd(&z.strm)
	z.err = io.EOF
	return nil
}

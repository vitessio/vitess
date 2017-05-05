/*
Copyright 2017 Google Inc.

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

package cgzip

// See http://www.zlib.net/zlib_how.html for more information on this

/*
#cgo CFLAGS: -Werror=implicit
#cgo pkg-config: zlib

#include "zlib.h"

// inflateInit2 is a macro, so using a wrapper function
int zstream_inflate_init(char *strm) {
  ((z_stream*)strm)->zalloc = Z_NULL;
  ((z_stream*)strm)->zfree = Z_NULL;
  ((z_stream*)strm)->opaque = Z_NULL;
  ((z_stream*)strm)->avail_in = 0;
  ((z_stream*)strm)->next_in = Z_NULL;
  return inflateInit2((z_stream*)strm,
                      16+15); // 16 makes it understand only gzip files
}

// deflateInit2 is a macro, so using a wrapper function
// using deflateInit2 instead of deflateInit to be able to specify gzip format
int zstream_deflate_init(char *strm, int level) {
  ((z_stream*)strm)->zalloc = Z_NULL;
  ((z_stream*)strm)->zfree = Z_NULL;
  ((z_stream*)strm)->opaque = Z_NULL;
  return deflateInit2((z_stream*)strm, level, Z_DEFLATED,
                      16+15, // 16 makes it a gzip file, 15 is default
                      8, Z_DEFAULT_STRATEGY); // default values
}

unsigned int zstream_avail_in(char *strm) {
  return ((z_stream*)strm)->avail_in;
}

unsigned int zstream_avail_out(char *strm) {
  return ((z_stream*)strm)->avail_out;
}

char* zstream_msg(char *strm) {
  return ((z_stream*)strm)->msg;
}

void zstream_set_in_buf(char *strm, void *buf, unsigned int len) {
  ((z_stream*)strm)->next_in = (Bytef*)buf;
  ((z_stream*)strm)->avail_in = len;
}

void zstream_set_out_buf(char *strm, void *buf, unsigned int len) {
  ((z_stream*)strm)->next_out = (Bytef*)buf;
  ((z_stream*)strm)->avail_out = len;
}

int zstream_inflate(char *strm, int flag) {
  return inflate((z_stream*)strm, flag);
}

int zstream_deflate(char *strm, int flag) {
  return deflate((z_stream*)strm, flag);
}

void zstream_inflate_end(char *strm) {
  inflateEnd((z_stream*)strm);
}

void zstream_deflate_end(char *strm) {
  deflateEnd((z_stream*)strm);
}
*/
import "C"

import (
	"fmt"
	"unsafe"
)

const (
	zNoFlush = C.Z_NO_FLUSH
)

// z_stream is a buffer that's big enough to fit a C.z_stream.
// This lets us allocate a C.z_stream within Go, while keeping the contents
// opaque to the Go GC. Otherwise, the GC would look inside and complain that
// the pointers are invalid, since they point to objects allocated by C code.
type zstream [unsafe.Sizeof(C.z_stream{})]C.char

func (strm *zstream) inflateInit() error {
	result := C.zstream_inflate_init(&strm[0])
	if result != Z_OK {
		return fmt.Errorf("cgzip: failed to initialize inflate (%v): %v", result, strm.msg())
	}
	return nil
}

func (strm *zstream) deflateInit(level int) error {
	result := C.zstream_deflate_init(&strm[0], C.int(level))
	if result != Z_OK {
		return fmt.Errorf("cgzip: failed to initialize deflate (%v): %v", result, strm.msg())
	}
	return nil
}

func (strm *zstream) inflateEnd() {
	C.zstream_inflate_end(&strm[0])
}

func (strm *zstream) deflateEnd() {
	C.zstream_deflate_end(&strm[0])
}

func (strm *zstream) availIn() int {
	return int(C.zstream_avail_in(&strm[0]))
}

func (strm *zstream) availOut() int {
	return int(C.zstream_avail_out(&strm[0]))
}

func (strm *zstream) msg() string {
	return C.GoString(C.zstream_msg(&strm[0]))
}

func (strm *zstream) setInBuf(buf []byte, size int) {
	if buf == nil {
		C.zstream_set_in_buf(&strm[0], nil, C.uint(size))
	} else {
		C.zstream_set_in_buf(&strm[0], unsafe.Pointer(&buf[0]), C.uint(size))
	}
}

func (strm *zstream) setOutBuf(buf []byte, size int) {
	if buf == nil {
		C.zstream_set_out_buf(&strm[0], nil, C.uint(size))
	} else {
		C.zstream_set_out_buf(&strm[0], unsafe.Pointer(&buf[0]), C.uint(size))
	}
}

func (strm *zstream) inflate(flag int) (int, error) {
	ret := C.zstream_inflate(&strm[0], C.int(flag))
	switch ret {
	case Z_NEED_DICT:
		ret = Z_DATA_ERROR
		fallthrough
	case Z_DATA_ERROR, Z_MEM_ERROR:
		return int(ret), fmt.Errorf("cgzip: failed to inflate (%v): %v", ret, strm.msg())
	}
	return int(ret), nil
}

func (strm *zstream) deflate(flag int) {
	ret := C.zstream_deflate(&strm[0], C.int(flag))
	if ret == Z_STREAM_ERROR {
		// all the other error cases are normal,
		// and this should never happen
		panic(fmt.Errorf("cgzip: Unexpected error (1)"))
	}
}

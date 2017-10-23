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

import "io"

// err starts out as nil
// we will call inflateEnd when we set err to a value:
// - whatever error is returned by the underlying reader
// - io.EOF if Close was called
type reader struct {
	r      io.Reader
	in     []byte
	strm   zstream
	err    error
	skipIn bool
}

// NewReader returns a new cgzip.reader for reading gzip files with the C gzip
// library.
func NewReader(r io.Reader) (io.ReadCloser, error) {
	return NewReaderBuffer(r, DEFAULT_COMPRESSED_BUFFER_SIZE)
}

// NewReaderBuffer returns a new cgzip.reader with a given buffer size for
// reading gzip files with the C gzip library.
func NewReaderBuffer(r io.Reader, bufferSize int) (io.ReadCloser, error) {
	z := &reader{r: r, in: make([]byte, bufferSize)}
	if err := z.strm.inflateInit(); err != nil {
		return nil, err
	}
	return z, nil
}

// Read reads from the gz stream.
func (z *reader) Read(p []byte) (int, error) {
	if z.err != nil {
		return 0, z.err
	}

	if len(p) == 0 {
		return 0, nil
	}

	// read and deflate until the output buffer is full
	z.strm.setOutBuf(p, len(p))

	for {
		// if we have no data to inflate, read more
		if !z.skipIn && z.strm.availIn() == 0 {
			var n int
			n, z.err = z.r.Read(z.in)
			// If we got data and EOF, pretend we didn't get the
			// EOF.  That way we will return the right values
			// upstream.  Note this will trigger another read
			// later on, that should return (0, EOF).
			if n > 0 && z.err == io.EOF {
				z.err = nil
			}

			// FIXME(alainjobart) this code is not compliant with
			// the Reader interface. We should process all the
			// data we got from the reader, and then return the
			// error, whatever it is.
			if (z.err != nil && z.err != io.EOF) || (n == 0 && z.err == io.EOF) {
				z.strm.inflateEnd()
				return 0, z.err
			}

			z.strm.setInBuf(z.in, n)
		} else {
			z.skipIn = false
		}

		// inflate some
		ret, err := z.strm.inflate(zNoFlush)
		if err != nil {
			z.err = err
			z.strm.inflateEnd()
			return 0, z.err
		}

		// if we read something, we're good
		have := len(p) - z.strm.availOut()
		if have > 0 {
			z.skipIn = ret == Z_OK && z.strm.availOut() == 0
			return have, z.err
		}
	}
}

// Close closes the Reader. It does not close the underlying io.Reader.
func (z *reader) Close() error {
	if z.err != nil {
		if z.err != io.EOF {
			return z.err
		}
		return nil
	}
	z.strm.inflateEnd()
	z.err = io.EOF
	return nil
}

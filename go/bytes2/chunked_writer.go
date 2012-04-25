/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

// Package bytes2 gives you alternate implementations of functionality
// similar to go's bytes package

package bytes2

import (
	"code.google.com/p/vitess/go/hack"
	"io"
	"unicode/utf8"
)

// ChunkedWriter has the same interface as bytes.Buffer's write functions.
// It additionally provides a Reserve function that returns a []byte that
// the caller can directly change.
type ChunkedWriter struct {
	bufs [][]byte
}

func NewChunkedWriter(chunkSize int) *ChunkedWriter {
	cw := &ChunkedWriter{make([][]byte, 1)}
	cw.bufs[0] = make([]byte, 0, chunkSize)
	return cw
}

// Bytes This function can get expensive for large buffers.
func (cw *ChunkedWriter) Bytes() (b []byte) {
	if len(cw.bufs) == 1 {
		return cw.bufs[0]
	}
	b = make([]byte, 0, cw.Len())
	for _, buf := range cw.bufs {
		b = append(b, buf...)
	}
	return b
}

func (cw *ChunkedWriter) Len() int {
	l := 0
	for _, buf := range cw.bufs {
		l += len(buf)
	}
	return l
}

func (cw *ChunkedWriter) Reset() {
	cw.bufs[0] = cw.bufs[0][:0]
	cw.bufs = cw.bufs[:1]
}

func (cw *ChunkedWriter) Truncate(n int) {
	for i, buf := range cw.bufs {
		if n > len(buf) {
			n -= len(buf)
			continue
		}
		cw.bufs[i] = buf[:n]
		cw.bufs = cw.bufs[:i+1]
		return
	}
	panic("bytes.ChunkedBuffer: truncation out of range")
}

func (cw *ChunkedWriter) Write(p []byte) (n int, err error) {
	return cw.WriteString(hack.String(p))
}

func (cw *ChunkedWriter) WriteString(p string) (n int, err error) {
	n = len(p)
	lastbuf := cw.bufs[len(cw.bufs)-1]
	for {
		available := cap(lastbuf) - len(lastbuf)
		required := len(p)
		if available >= required {
			cw.bufs[len(cw.bufs)-1] = append(lastbuf, p...)
			return
		}
		cw.bufs[len(cw.bufs)-1] = append(lastbuf, p[:available]...)
		p = p[available:]
		lastbuf = make([]byte, 0, cap(cw.bufs[0]))
		cw.bufs = append(cw.bufs, lastbuf)
	}
	return n, nil
}

func (cw *ChunkedWriter) Reserve(n int) (b []byte) {
	if n > cap(cw.bufs[0]) {
		panic("bytes.ChunkedBuffer: Reserve request too high")
	}
	lastbuf := cw.bufs[len(cw.bufs)-1]
	if n > cap(lastbuf)-len(lastbuf) {
		b = make([]byte, n, cap(cw.bufs[0]))
		cw.bufs = append(cw.bufs, b)
		return b
	}
	l := len(lastbuf)
	b = lastbuf[l : n+l]
	cw.bufs[len(cw.bufs)-1] = lastbuf[:n+l]
	return b
}

func (cw *ChunkedWriter) WriteByte(c byte) error {
	cw.Reserve(1)[0] = c
	return nil
}

func (cw *ChunkedWriter) WriteRune(r rune) (n int, err error) {
	n = utf8.EncodeRune(cw.Reserve(utf8.RuneLen(r)), r)
	return n, nil
}

func (cw *ChunkedWriter) WriteTo(w io.Writer) (n int64, err error) {
	for _, buf := range cw.bufs {
		m, err := w.Write(buf)
		n += int64(m)
		if err != nil {
			return n, err
		}
		if m != len(buf) {
			return n, io.ErrShortWrite
		}
	}
	cw.Reset()
	return n, nil
}

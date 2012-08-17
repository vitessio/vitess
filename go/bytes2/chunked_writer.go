// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package bytes2 gives you alternate implementations of functionality
// similar to go's bytes package

package bytes2

import (
	"code.google.com/p/vitess/go/hack"
	"fmt"
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
	b := cw.bufs[0][:0]
	cw.bufs = make([][]byte, 1)
	cw.bufs[0] = b
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
		panic(fmt.Sprintf("bytes.ChunkedBuffer: Reserve request too high: %d > %d", n, cap(cw.bufs[0])))
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

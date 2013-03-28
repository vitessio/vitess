// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package bufio implements buffered I/O.  It wraps an io.Reader or io.Writer
// object, creating another object (Reader or AsyncWriter) that also implements
// the interface but provides buffering and some help for textual I/O.
package bufio2

import (
	"io"
	"unicode/utf8"
)

const (
	defaultBufSize  = 4096
	defaultBufCount = 4
)

// buffered output

// AsyncWriter implements buffering for an io.Writer object.
// If an error occurs writing to a AsyncWriter, no more data will be
// accepted and all subsequent writes will return the error.
type AsyncWriter struct {
	// these are only touched by the input side
	err       error
	buf       []byte
	n         int
	size      int
	countSent int
	countRcvd int

	// these are only touched by the output side
	wr io.Writer

	// and this is synchronization
	dataChan   chan []byte
	resultChan chan error
}

// NewAsyncWriterSize returns a new AsyncWriter whose buffer has at least the specified
// size. If the argument io.Writer is already a AsyncWriter with large enough
// size, it returns the underlying AsyncWriter.
func NewAsyncWriterSize(wr io.Writer, size int, count int) *AsyncWriter {
	// Is it already a AsyncWriter?
	b, ok := wr.(*AsyncWriter)
	if ok && len(b.buf) >= size {
		return b
	}
	if size <= 0 {
		size = defaultBufSize
	}
	b = new(AsyncWriter)
	b.size = size
	b.buf = make([]byte, size)
	b.wr = wr
	if count <= 0 {
		count = defaultBufCount
	}
	b.dataChan = make(chan []byte, count)
	b.resultChan = make(chan error, count+1) // may have the one we're processing and count more waiting
	go func() {
		b.writeThread()
	}()
	return b
}

// NewAsyncWriter returns a new AsyncWriter whose buffer has the default size.
func NewAsyncWriter(wr io.Writer) *AsyncWriter {
	return NewAsyncWriterSize(wr, defaultBufSize, defaultBufCount)
}

func (b *AsyncWriter) writeThread() {
	var err error
	for buf := range b.dataChan {
		// we only write if we're not in error state
		if err == nil {
			var n int
			n, err = b.wr.Write(buf)
			if n < len(buf) && err == nil {
				// short writes are considered error for now
				err = io.ErrShortWrite
			}
		}

		// and write the result
		b.resultChan <- err
	}
}

// Takes whatever data we have in the buffer and sends it
// on the channel.
func (b *AsyncWriter) sendData() error {
	// flush the result channel to collect eventual errors
	haveResults := true
	for haveResults {
		select {
		case err := <-b.resultChan:
			b.countRcvd++
			if err != nil && b.err == nil {
				b.err = err
			}
		default:
			haveResults = false
		}
	}
	if b.err != nil {
		return b.err
	}

	// send the buffer if we have anything to send
	if b.n != 0 {
		b.countSent++
		b.dataChan <- b.buf[0:b.n]
		b.buf = make([]byte, b.size)
		b.n = 0
	}
	return nil
}

func (b *AsyncWriter) Flush() error {
	if err := b.sendData(); err != nil {
		return err
	}
	return b.WaitForWrites()
}

func (b *AsyncWriter) WaitForWrites() error {
	for b.countRcvd < b.countSent {
		err := <-b.resultChan
		b.countRcvd++
		if err != nil && b.err == nil {
			b.err = err
		}
	}
	return b.err
}

// Available returns how many bytes are unused in the buffer.
func (b *AsyncWriter) Available() int { return len(b.buf) - b.n }

// Buffered returns the number of bytes that have been written into the current buffer.
func (b *AsyncWriter) Buffered() int { return b.n }

// Write writes the contents of p into the buffer.
// It returns the number of bytes written.
// If nn < len(p), it also returns an error explaining
// why the write is short.
func (b *AsyncWriter) Write(p []byte) (nn int, err error) {
	for len(p) > b.Available() && b.err == nil {
		n := copy(b.buf[b.n:], p)
		b.n += n
		b.sendData()
		nn += n
		p = p[n:]
	}
	if b.err != nil {
		return nn, b.err
	}
	n := copy(b.buf[b.n:], p)
	b.n += n
	nn += n
	return nn, nil
}

// WriteByte writes a single byte.
func (b *AsyncWriter) WriteByte(c byte) error {
	if b.err != nil {
		return b.err
	}
	if b.Available() <= 0 && b.sendData() != nil {
		return b.err
	}
	b.buf[b.n] = c
	b.n++
	return nil
}

// WriteRune writes a single Unicode code point, returning
// the number of bytes written and any error.
func (b *AsyncWriter) WriteRune(r rune) (size int, err error) {
	if r < utf8.RuneSelf {
		err = b.WriteByte(byte(r))
		if err != nil {
			return 0, err
		}
		return 1, nil
	}
	if b.err != nil {
		return 0, b.err
	}
	n := b.Available()
	if n < utf8.UTFMax {
		if b.sendData(); b.err != nil {
			return 0, b.err
		}
		n = b.Available()
		if n < utf8.UTFMax {
			// Can only happen if buffer is silly small.
			return b.WriteString(string(r))
		}
	}
	size = utf8.EncodeRune(b.buf[b.n:], r)
	b.n += size
	return size, nil
}

// WriteString writes a string.
// It returns the number of bytes written.
// If the count is less than len(s), it also returns an error explaining
// why the write is short.
func (b *AsyncWriter) WriteString(s string) (int, error) {
	nn := 0
	for len(s) > b.Available() && b.err == nil {
		n := copy(b.buf[b.n:], s)
		b.n += n
		nn += n
		s = s[n:]
		b.sendData()
	}
	if b.err != nil {
		return nn, b.err
	}
	n := copy(b.buf[b.n:], s)
	b.n += n
	nn += n
	return nn, nil
}

// ReadFrom implements io.ReaderFrom.
func (b *AsyncWriter) ReadFrom(r io.Reader) (n int64, err error) {
	if b.Buffered() == 0 {
		if w, ok := b.wr.(io.ReaderFrom); ok {
			return w.ReadFrom(r)
		}
	}
	var m int
	for {
		m, err = r.Read(b.buf[b.n:])
		if m == 0 {
			break
		}
		b.n += m
		n += int64(m)
		if b.Available() == 0 {
			if err1 := b.sendData(); err1 != nil {
				return n, err1
			}
		}
		if err != nil {
			break
		}
	}
	if err == io.EOF {
		err = nil
	}
	return n, err
}

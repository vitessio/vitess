package mysql

import (
	"bufio"
	"io"
	"sync"
)

// Writer implementation
// This writer gets *bufio.Writer from pool on Write if it has no one already and
// puts it back in pool on Flush().
// Because our Connection implementation calls Flush() after each protocol write,
// we can expect every *bufio.Writer to return back to pool eventually.
// You can find c.flush() calls in writeEndResult and writeOKPacketWithEOFHeader.
// One of those functions always concludes writing result.

var writersPool = sync.Pool{New: func() interface{} { return bufio.NewWriterSize(nil, connBufferSize) }}

type bufioWriter interface {
	Write([]byte) (int, error)
	Reset(io.Writer)
	Flush() error
}

func (pbw *poolBufioWriter) getWriter() {
	if pbw.bw != nil {
		return
	}
	pbw.bw = writersPool.Get().(*bufio.Writer)
	pbw.bw.Reset(pbw.w)
}

func (pbw *poolBufioWriter) putWriter() {
	if pbw.bw == nil {
		return
	}
	// remove reference
	pbw.bw.Reset(nil)
	writersPool.Put(pbw.bw)
	pbw.bw = nil
}

type poolBufioWriter struct {
	w  io.Writer
	bw *bufio.Writer
}

func newWriter(w io.Writer) bufioWriter {
	return &poolBufioWriter{
		w: w,
	}
}

func (pbw *poolBufioWriter) Write(b []byte) (int, error) {
	pbw.getWriter()
	return pbw.bw.Write(b)
}

func (pbw *poolBufioWriter) Reset(w io.Writer) {
	pbw.putWriter()
	pbw.w = w
}

func (pbw *poolBufioWriter) Flush() error {
	if pbw.bw == nil {
		return nil
	}
	err := pbw.bw.Flush()
	pbw.putWriter()
	return err
}

package mysql

import (
	"bufio"
	"io"
	"sync"
)

// Reader implementation

type bufioReader interface {
	Read([]byte) (int, error)
	Reset(io.Reader)
}

var readersPool = sync.Pool{New: func() interface{} { return bufio.NewReaderSize(nil, connBufferSize) }}

func (pbr *poolBufioReader) getReader() {
	if pbr.br != nil {
		return
	}
	pbr.br = readersPool.Get().(*bufio.Reader)
	pbr.br.Reset(pbr.r)
}

func (pbr *poolBufioReader) putReader() {
	if pbr.br == nil {
		return
	}
	// remove reference
	pbr.br.Reset(nil)
	readersPool.Put(pbr.br)
	pbr.br = nil
}

type poolBufioReader struct {
	r  io.Reader
	br *bufio.Reader
}

func newReader(r io.Reader) bufioReader {
	return &poolBufioReader{
		r: r,
	}
}

func (pbr *poolBufioReader) Read(b []byte) (int, error) {
	pbr.getReader()
	n, err := pbr.br.Read(b)
	if pbr.br.Buffered() == 0 {
		pbr.putReader()
	}
	return n, err
}

func (pbr *poolBufioReader) Reset(r io.Reader) {
	pbr.putReader()
	pbr.r = r
}

// Writer implementation

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

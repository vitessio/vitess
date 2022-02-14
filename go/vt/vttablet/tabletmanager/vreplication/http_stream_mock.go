/*
 * To be used instead of HttpTest in cases where a test needs to do synchronized sequential reads are required from the
 * http stream. Note that this handles only one write at a time: the test should read the written data before the next
 * write. Data is sent to the channel only on a Flush.
 */

package vreplication

import (
	"net/http"
)

// HTTPStreamWriterMock implements http.ResponseWriter and adds a channel to sync writes and reads
type HTTPStreamWriterMock struct {
	ch   chan interface{}
	data []byte
}

// NewHTTPStreamWriterMock returns a new HTTPStreamWriterMock
func NewHTTPStreamWriterMock() *HTTPStreamWriterMock {
	return &HTTPStreamWriterMock{ch: make(chan interface{}, 1), data: make([]byte, 0)}
}

// Header is a stub
func (w *HTTPStreamWriterMock) Header() http.Header {
	return nil
}

// WriteHeader is a stub
func (w *HTTPStreamWriterMock) WriteHeader(statuscode int) {
}

// Write buffers sent data
func (w *HTTPStreamWriterMock) Write(data []byte) (int, error) {
	w.data = append(w.data, data...)
	return 0, nil
}

// Flush sends buffered data to the channel
func (w *HTTPStreamWriterMock) Flush() {
	w.ch <- w.data
	w.data = w.data[:0]
}

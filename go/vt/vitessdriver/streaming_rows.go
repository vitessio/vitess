// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vitessdriver

import (
	"database/sql/driver"
	"errors"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// streamingRows creates a database/sql/driver compliant Row iterator
// for a streaming query.
type streamingRows struct {
	stream sqltypes.ResultStream
	failed error
	fields []*querypb.Field
	qr     *sqltypes.Result
	index  int
	cancel context.CancelFunc
}

// newStreamingRows creates a new streamingRows from stream.
func newStreamingRows(stream sqltypes.ResultStream, cancel context.CancelFunc) driver.Rows {
	return &streamingRows{
		stream: stream,
		cancel: cancel,
	}
}

func (ri *streamingRows) Columns() []string {
	ri.checkFields()
	if ri.failed != nil {
		return nil
	}
	cols := make([]string, 0, len(ri.fields))
	for _, field := range ri.fields {
		cols = append(cols, field.Name)
	}
	return cols
}

func (ri *streamingRows) Close() error {
	return nil
}

func (ri *streamingRows) Next(dest []driver.Value) error {
	ri.checkFields()
	if ri.failed != nil {
		return ri.failed
	}
	for ri.qr == nil || ri.index == len(ri.qr.Rows) {
		ri.fetchNext()
		if ri.failed != nil {
			return ri.failed
		}
		ri.index = 0
	}
	populateRow(dest, ri.qr.Rows[ri.index])
	ri.index++
	return nil
}

// checkFields fetches the first packet from the channel, which
// should contain the field info. It sets ri.failed if it fails.
func (ri *streamingRows) checkFields() {
	if ri.failed != nil {
		return
	}
	if ri.fields != nil {
		return
	}
	qr, err := ri.stream.Recv()
	if err != nil {
		ri.setErr(err)
		return
	}
	ri.fields = qr.Fields
	if ri.fields == nil {
		ri.failed = errors.New("first packet did not return fields")
	}
}

// fetchNext fetches the next packet from the channel.
func (ri *streamingRows) fetchNext() {
	qr, err := ri.stream.Recv()
	if err != nil {
		ri.setErr(err)
		return
	}
	ri.qr = qr
}

func (ri *streamingRows) setErr(err error) {
	ri.failed = err
	if ri.cancel != nil {
		ri.cancel()
	}
}

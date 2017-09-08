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
	stream  sqltypes.ResultStream
	failed  error
	fields  []*querypb.Field
	qr      *sqltypes.Result
	index   int
	cancel  context.CancelFunc
	convert *converter
}

// newStreamingRows creates a new streamingRows from stream.
func newStreamingRows(stream sqltypes.ResultStream, cancel context.CancelFunc, conv *converter) driver.Rows {
	return &streamingRows{
		stream:  stream,
		cancel:  cancel,
		convert: conv,
	}
}

func (ri *streamingRows) Columns() []string {
	if ri.failed != nil {
		return nil
	}
	if err := ri.checkFields(); err != nil {
		_ = ri.setErr(err)
		return nil
	}
	cols := make([]string, 0, len(ri.fields))
	for _, field := range ri.fields {
		cols = append(cols, field.Name)
	}
	return cols
}

func (ri *streamingRows) Close() error {
	if ri.cancel != nil {
		ri.cancel()
	}
	return nil
}

func (ri *streamingRows) Next(dest []driver.Value) error {
	if ri.failed != nil {
		return ri.failed
	}
	if err := ri.checkFields(); err != nil {
		return ri.setErr(err)
	}
	// If no results were fetched or rows exhausted,
	// loop until we get a non-zero number of rows.
	for ri.qr == nil || ri.index >= len(ri.qr.Rows) {
		qr, err := ri.stream.Recv()
		if err != nil {
			return ri.setErr(err)
		}
		ri.qr = qr
		ri.index = 0
	}
	if err := ri.convert.populateRow(dest, ri.qr.Rows[ri.index]); err != nil {
		return err
	}
	ri.index++
	return nil
}

// checkFields fetches the first packet from the channel, which
// should contain the field info.
func (ri *streamingRows) checkFields() error {
	if ri.fields != nil {
		return nil
	}
	qr, err := ri.stream.Recv()
	if err != nil {
		return err
	}
	ri.fields = qr.Fields
	if ri.fields == nil {
		return errors.New("first packet did not return fields")
	}
	return nil
}

func (ri *streamingRows) setErr(err error) error {
	ri.failed = err
	return err
}

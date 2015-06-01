// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package client

import (
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
)

var packet1 = mproto.QueryResult{
	Fields: []mproto.Field{
		mproto.Field{
			Name: "field1",
			Type: mproto.VT_LONG,
		},
		mproto.Field{
			Name: "field2",
			Type: mproto.VT_FLOAT,
		},
		mproto.Field{
			Name: "field3",
			Type: mproto.VT_VAR_STRING,
		},
	},
}

var packet2 = mproto.QueryResult{
	Rows: [][]sqltypes.Value{
		[]sqltypes.Value{
			sqltypes.MakeString([]byte("1")),
			sqltypes.MakeString([]byte("1.1")),
			sqltypes.MakeString([]byte("value1")),
		},
	},
}

var packet3 = mproto.QueryResult{
	Rows: [][]sqltypes.Value{
		[]sqltypes.Value{
			sqltypes.MakeString([]byte("2")),
			sqltypes.MakeString([]byte("2.2")),
			sqltypes.MakeString([]byte("value2")),
		},
	},
}

func TestStreamingRows(t *testing.T) {
	qrc, errFunc := func() (<-chan *mproto.QueryResult, vtgateconn.ErrFunc) {
		ch := make(chan *mproto.QueryResult)
		go func() {
			ch <- &packet1
			ch <- &packet2
			ch <- &packet3
			close(ch)
		}()
		return ch, func() error { return nil }
	}()
	ri := newStreamingRows(qrc, errFunc)
	wantCols := []string{
		"field1",
		"field2",
		"field3",
	}
	gotCols := ri.Columns()
	if !reflect.DeepEqual(gotCols, wantCols) {
		t.Errorf("cols: %v, want %v", gotCols, wantCols)
	}

	wantRow := []driver.Value{
		int64(1),
		float64(1.1),
		[]byte("value1"),
	}
	gotRow := make([]driver.Value, 3)
	err := ri.Next(gotRow)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(gotRow, wantRow) {
		t.Errorf("row1: %v, want %v", gotRow, wantRow)
	}

	wantRow = []driver.Value{
		int64(2),
		float64(2.2),
		[]byte("value2"),
	}
	err = ri.Next(gotRow)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(gotRow, wantRow) {
		t.Errorf("row1: %v, want %v", gotRow, wantRow)
	}

	err = ri.Next(gotRow)
	if err != io.EOF {
		t.Errorf("got: %v, want %v", err, io.EOF)
	}

	_ = ri.Close()
}

func TestStreamingRowsReversed(t *testing.T) {
	qrc, errFunc := func() (<-chan *mproto.QueryResult, vtgateconn.ErrFunc) {
		ch := make(chan *mproto.QueryResult)
		go func() {
			ch <- &packet1
			ch <- &packet2
			ch <- &packet3
			close(ch)
		}()
		return ch, func() error { return nil }
	}()
	ri := newStreamingRows(qrc, errFunc)

	wantRow := []driver.Value{
		int64(1),
		float64(1.1),
		[]byte("value1"),
	}
	gotRow := make([]driver.Value, 3)
	err := ri.Next(gotRow)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(gotRow, wantRow) {
		t.Errorf("row1: %v, want %v", gotRow, wantRow)
	}

	wantCols := []string{
		"field1",
		"field2",
		"field3",
	}
	gotCols := ri.Columns()
	if !reflect.DeepEqual(gotCols, wantCols) {
		t.Errorf("cols: %v, want %v", gotCols, wantCols)
	}

	_ = ri.Close()
}

func TestStreamingRowsError(t *testing.T) {
	qrc, errFunc := func() (<-chan *mproto.QueryResult, vtgateconn.ErrFunc) {
		ch := make(chan *mproto.QueryResult)
		go func() {
			close(ch)
		}()
		return ch, func() error { return errors.New("error before fields") }
	}()
	ri := newStreamingRows(qrc, errFunc)
	gotCols := ri.Columns()
	if gotCols != nil {
		t.Errorf("cols: %v, want nil", gotCols)
	}
	gotRow := make([]driver.Value, 3)
	err := ri.Next(gotRow)
	wantErr := "error before fields"
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Errorf("err: %v does not contain %v", err, wantErr)
	}
	_ = ri.Close()

	qrc, errFunc = func() (<-chan *mproto.QueryResult, vtgateconn.ErrFunc) {
		ch := make(chan *mproto.QueryResult)
		go func() {
			ch <- &packet1
			close(ch)
		}()
		return ch, func() error { return errors.New("error after fields") }
	}()
	ri = newStreamingRows(qrc, errFunc)
	wantCols := []string{
		"field1",
		"field2",
		"field3",
	}
	gotCols = ri.Columns()
	if !reflect.DeepEqual(gotCols, wantCols) {
		t.Errorf("cols: %v, want %v", gotCols, wantCols)
	}
	gotRow = make([]driver.Value, 3)
	err = ri.Next(gotRow)
	wantErr = "error after fields"
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Errorf("err: %v does not contain %v", err, wantErr)
	}
	// Ensure error persists.
	err = ri.Next(gotRow)
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Errorf("err: %v does not contain %v", err, wantErr)
	}
	_ = ri.Close()

	qrc, errFunc = func() (<-chan *mproto.QueryResult, vtgateconn.ErrFunc) {
		ch := make(chan *mproto.QueryResult)
		go func() {
			ch <- &packet1
			ch <- &packet2
			close(ch)
		}()
		return ch, func() error { return errors.New("error after rows") }
	}()
	ri = newStreamingRows(qrc, errFunc)
	gotRow = make([]driver.Value, 3)
	err = ri.Next(gotRow)
	if err != nil {
		t.Error(err)
	}
	err = ri.Next(gotRow)
	wantErr = "error after rows"
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Errorf("err: %v does not contain %v", err, wantErr)
	}
	_ = ri.Close()

	qrc, errFunc = func() (<-chan *mproto.QueryResult, vtgateconn.ErrFunc) {
		ch := make(chan *mproto.QueryResult)
		go func() {
			ch <- &packet2
			close(ch)
		}()
		return ch, func() error { return nil }
	}()
	ri = newStreamingRows(qrc, errFunc)
	gotRow = make([]driver.Value, 3)
	err = ri.Next(gotRow)
	wantErr = "first packet did not return fields"
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Errorf("err: %v does not contain %v", err, wantErr)
	}
	_ = ri.Close()
}

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

	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
)

var packet1 = sqltypes.Result{
	Fields: []*querypb.Field{
		&querypb.Field{
			Name: "field1",
			Type: sqltypes.Int32,
		},
		&querypb.Field{
			Name: "field2",
			Type: sqltypes.Float32,
		},
		&querypb.Field{
			Name: "field3",
			Type: sqltypes.VarChar,
		},
	},
}

var packet2 = sqltypes.Result{
	Rows: [][]sqltypes.Value{
		[]sqltypes.Value{
			sqltypes.MakeTrusted(sqltypes.Int32, []byte("1")),
			sqltypes.MakeTrusted(sqltypes.Float32, []byte("1.1")),
			sqltypes.MakeTrusted(sqltypes.VarChar, []byte("value1")),
		},
	},
}

var packet3 = sqltypes.Result{
	Rows: [][]sqltypes.Value{
		[]sqltypes.Value{
			sqltypes.MakeTrusted(sqltypes.Int32, []byte("2")),
			sqltypes.MakeTrusted(sqltypes.Float32, []byte("2.2")),
			sqltypes.MakeTrusted(sqltypes.VarChar, []byte("value2")),
		},
	},
}

func TestStreamingRows(t *testing.T) {
	qrc, errFunc := func() (<-chan *sqltypes.Result, vtgateconn.ErrFunc) {
		ch := make(chan *sqltypes.Result)
		go func() {
			ch <- &packet1
			ch <- &packet2
			ch <- &packet3
			close(ch)
		}()
		return ch, func() error { return nil }
	}()
	ri := newStreamingRows(qrc, errFunc, nil)
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
	qrc, errFunc := func() (<-chan *sqltypes.Result, vtgateconn.ErrFunc) {
		ch := make(chan *sqltypes.Result)
		go func() {
			ch <- &packet1
			ch <- &packet2
			ch <- &packet3
			close(ch)
		}()
		return ch, func() error { return nil }
	}()
	ri := newStreamingRows(qrc, errFunc, nil)

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
	qrc, errFunc := func() (<-chan *sqltypes.Result, vtgateconn.ErrFunc) {
		ch := make(chan *sqltypes.Result)
		go func() {
			close(ch)
		}()
		return ch, func() error { return errors.New("error before fields") }
	}()
	ri := newStreamingRows(qrc, errFunc, nil)
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

	qrc, errFunc = func() (<-chan *sqltypes.Result, vtgateconn.ErrFunc) {
		ch := make(chan *sqltypes.Result)
		go func() {
			ch <- &packet1
			close(ch)
		}()
		return ch, func() error { return errors.New("error after fields") }
	}()
	ri = newStreamingRows(qrc, errFunc, nil)
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

	qrc, errFunc = func() (<-chan *sqltypes.Result, vtgateconn.ErrFunc) {
		ch := make(chan *sqltypes.Result)
		go func() {
			ch <- &packet1
			ch <- &packet2
			close(ch)
		}()
		return ch, func() error { return errors.New("error after rows") }
	}()
	ri = newStreamingRows(qrc, errFunc, nil)
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

	qrc, errFunc = func() (<-chan *sqltypes.Result, vtgateconn.ErrFunc) {
		ch := make(chan *sqltypes.Result)
		go func() {
			ch <- &packet2
			close(ch)
		}()
		return ch, func() error { return nil }
	}()
	ri = newStreamingRows(qrc, errFunc, nil)
	gotRow = make([]driver.Value, 3)
	err = ri.Next(gotRow)
	wantErr = "first packet did not return fields"
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Errorf("err: %v does not contain %v", err, wantErr)
	}
	_ = ri.Close()
}

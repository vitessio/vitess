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
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

var packet1 = sqltypes.Result{
	Fields: []*querypb.Field{
		{
			Name: "field1",
			Type: sqltypes.Int32,
		},
		{
			Name: "field2",
			Type: sqltypes.Float32,
		},
		{
			Name: "field3",
			Type: sqltypes.VarChar,
		},
	},
}

var packet2 = sqltypes.Result{
	Rows: [][]sqltypes.Value{
		{
			sqltypes.NewInt32(1),
			sqltypes.TestValue(sqltypes.Float32, "1.1"),
			sqltypes.NewVarChar("value1"),
		},
	},
}

var packet3 = sqltypes.Result{
	Rows: [][]sqltypes.Value{
		{
			sqltypes.NewInt32(2),
			sqltypes.TestValue(sqltypes.Float32, "2.2"),
			sqltypes.NewVarChar("value2"),
		},
	},
}

type adapter struct {
	c   chan *sqltypes.Result
	err error
}

func (a *adapter) Recv() (*sqltypes.Result, error) {
	r, ok := <-a.c
	if !ok {
		return nil, a.err
	}
	return r, nil
}

func TestStreamingRows(t *testing.T) {
	c := make(chan *sqltypes.Result, 3)
	c <- &packet1
	c <- &packet2
	c <- &packet3
	close(c)
	ri := newStreamingRows(&adapter{c: c, err: io.EOF}, nil, &converter{})
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
	c := make(chan *sqltypes.Result, 3)
	c <- &packet1
	c <- &packet2
	c <- &packet3
	close(c)
	ri := newStreamingRows(&adapter{c: c, err: io.EOF}, nil, &converter{})
	defer ri.Close()

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
	c := make(chan *sqltypes.Result)
	close(c)
	ri := newStreamingRows(&adapter{c: c, err: errors.New("error before fields")}, nil, &converter{})

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

	c = make(chan *sqltypes.Result, 1)
	c <- &packet1
	close(c)
	ri = newStreamingRows(&adapter{c: c, err: errors.New("error after fields")}, nil, &converter{})
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

	c = make(chan *sqltypes.Result, 2)
	c <- &packet1
	c <- &packet2
	close(c)
	ri = newStreamingRows(&adapter{c: c, err: errors.New("error after rows")}, nil, &converter{})
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

	c = make(chan *sqltypes.Result, 1)
	c <- &packet2
	close(c)
	ri = newStreamingRows(&adapter{c: c, err: io.EOF}, nil, &converter{})
	gotRow = make([]driver.Value, 3)
	err = ri.Next(gotRow)
	wantErr = "first packet did not return fields"
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Errorf("err: %v does not contain %v", err, wantErr)
	}
	_ = ri.Close()
}

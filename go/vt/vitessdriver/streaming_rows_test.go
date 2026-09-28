/*
Copyright 2019 The Vitess Authors.

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
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
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
	ri := newStreamingRows(&adapter{c: c, err: io.EOF}, &converter{})
	wantCols := []string{
		"field1",
		"field2",
		"field3",
	}
	gotCols := ri.Columns()
	require.Equal(t, wantCols, gotCols)

	wantRow := []driver.Value{
		int64(1),
		float64(1.1),
		[]byte("value1"),
	}
	gotRow := make([]driver.Value, 3)
	err := ri.Next(gotRow)
	require.NoError(t, err)
	require.Equal(t, wantRow, gotRow)

	wantRow = []driver.Value{
		int64(2),
		float64(2.2),
		[]byte("value2"),
	}
	err = ri.Next(gotRow)
	require.NoError(t, err)
	require.Equal(t, wantRow, gotRow)

	err = ri.Next(gotRow)
	require.ErrorIs(t, err, io.EOF)

	_ = ri.Close()
}

func TestStreamingRowsReversed(t *testing.T) {
	c := make(chan *sqltypes.Result, 3)
	c <- &packet1
	c <- &packet2
	c <- &packet3
	close(c)
	ri := newStreamingRows(&adapter{c: c, err: io.EOF}, &converter{})
	defer ri.Close()

	wantRow := []driver.Value{
		int64(1),
		float64(1.1),
		[]byte("value1"),
	}
	gotRow := make([]driver.Value, 3)
	err := ri.Next(gotRow)
	require.NoError(t, err)
	require.Equal(t, wantRow, gotRow)

	wantCols := []string{
		"field1",
		"field2",
		"field3",
	}
	gotCols := ri.Columns()
	require.Equal(t, wantCols, gotCols)

	_ = ri.Close()
}

func TestStreamingRowsError(t *testing.T) {
	c := make(chan *sqltypes.Result)
	close(c)
	ri := newStreamingRows(&adapter{c: c, err: errors.New("error before fields")}, &converter{})

	gotCols := ri.Columns()
	require.Nil(t, gotCols)
	gotRow := make([]driver.Value, 3)
	err := ri.Next(gotRow)
	require.ErrorContains(t, err, "error before fields")
	_ = ri.Close()

	c = make(chan *sqltypes.Result, 1)
	c <- &packet1
	close(c)
	ri = newStreamingRows(&adapter{c: c, err: errors.New("error after fields")}, &converter{})
	wantCols := []string{
		"field1",
		"field2",
		"field3",
	}
	gotCols = ri.Columns()
	require.Equal(t, wantCols, gotCols)
	gotRow = make([]driver.Value, 3)
	err = ri.Next(gotRow)
	require.ErrorContains(t, err, "error after fields")
	// Ensure error persists.
	err = ri.Next(gotRow)
	require.ErrorContains(t, err, "error after fields")
	_ = ri.Close()

	c = make(chan *sqltypes.Result, 2)
	c <- &packet1
	c <- &packet2
	close(c)
	ri = newStreamingRows(&adapter{c: c, err: errors.New("error after rows")}, &converter{})
	gotRow = make([]driver.Value, 3)
	err = ri.Next(gotRow)
	require.NoError(t, err)
	err = ri.Next(gotRow)
	require.ErrorContains(t, err, "error after rows")
	_ = ri.Close()

	c = make(chan *sqltypes.Result, 1)
	c <- &packet2
	close(c)
	ri = newStreamingRows(&adapter{c: c, err: io.EOF}, &converter{})
	gotRow = make([]driver.Value, 3)
	err = ri.Next(gotRow)
	require.ErrorContains(t, err, "first packet did not return fields")
	_ = ri.Close()
}

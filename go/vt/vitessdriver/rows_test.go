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
	"io"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var rowsResult1 = sqltypes.Result{
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
		// Signed types which are smaller than uint64, will become an int64.
		{
			Name: "field4",
			Type: sqltypes.Uint32,
		},
		// Signed uint64 values must be mapped to uint64.
		{
			Name: "field5",
			Type: sqltypes.Uint64,
		},
	},
	RowsAffected: 2,
	InsertID:     0,
	Rows: [][]sqltypes.Value{
		{
			sqltypes.NewInt32(1),
			sqltypes.TestValue(sqltypes.Float32, "1.1"),
			sqltypes.NewVarChar("value1"),
			sqltypes.TestValue(sqltypes.Uint32, "2147483647"), // 2^31-1, NOT out of range for int32 => should become int64
			sqltypes.NewUint64(9223372036854775807),           // 2^63-1, NOT out of range for int64
		},
		{
			sqltypes.NewInt32(2),
			sqltypes.TestValue(sqltypes.Float32, "2.2"),
			sqltypes.NewVarChar("value2"),
			sqltypes.TestValue(sqltypes.Uint32, "4294967295"), // 2^32-1, out of range for int32 => should become int64
			sqltypes.NewUint64(18446744073709551615),          // 2^64-1, out of range for int64
		},
	},
}

func TestRows(t *testing.T) {
	ri := newRows(&rowsResult1, &converter{})
	wantCols := []string{
		"field1",
		"field2",
		"field3",
		"field4",
		"field5",
	}
	gotCols := ri.Columns()
	require.Equal(t, wantCols, gotCols)

	wantRow := []driver.Value{
		int64(1),
		float64(1.1),
		[]byte("value1"),
		uint64(2147483647),
		uint64(9223372036854775807),
	}
	gotRow := make([]driver.Value, len(wantRow))
	err := ri.Next(gotRow)
	require.NoError(t, err)
	require.Equal(t, wantRow, gotRow)

	wantRow = []driver.Value{
		int64(2),
		float64(2.2),
		[]byte("value2"),
		uint64(4294967295),
		uint64(18446744073709551615),
	}
	err = ri.Next(gotRow)
	require.NoError(t, err)
	require.Equal(t, wantRow, gotRow)

	err = ri.Next(gotRow)
	require.ErrorIs(t, err, io.EOF)

	_ = ri.Close()
}

// Test that the ColumnTypeScanType function returns the correct reflection type for each
// sql type. The sql type in turn comes from a table column's type.
func TestColumnTypeScanType(t *testing.T) {
	r := sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "field1",
				Type: sqltypes.Int8,
			},
			{
				Name: "field2",
				Type: sqltypes.Uint8,
			},
			{
				Name: "field3",
				Type: sqltypes.Int16,
			},
			{
				Name: "field4",
				Type: sqltypes.Uint16,
			},
			{
				Name: "field5",
				Type: sqltypes.Int24,
			},
			{
				Name: "field6",
				Type: sqltypes.Uint24,
			},
			{
				Name: "field7",
				Type: sqltypes.Int32,
			},
			{
				Name: "field8",
				Type: sqltypes.Uint32,
			},
			{
				Name: "field9",
				Type: sqltypes.Int64,
			},
			{
				Name: "field10",
				Type: sqltypes.Uint64,
			},
			{
				Name: "field11",
				Type: sqltypes.Float32,
			},
			{
				Name: "field12",
				Type: sqltypes.Float64,
			},
			{
				Name: "field13",
				Type: sqltypes.VarBinary,
			},
			{
				Name: "field14",
				Type: sqltypes.Datetime,
			},
		},
	}

	ri := newRows(&r, &converter{}).(driver.RowsColumnTypeScanType)
	defer ri.Close()

	wantTypes := []reflect.Type{
		typeInt8,
		typeUint8,
		typeInt16,
		typeUint16,
		typeInt32,
		typeUint32,
		typeInt32,
		typeUint32,
		typeInt64,
		typeUint64,
		typeFloat32,
		typeFloat64,
		typeRawBytes,
		typeTime,
	}

	for i := range wantTypes {
		require.Equal(t, wantTypes[i], ri.ColumnTypeScanType(i))
	}
}

// Test that the ColumnTypeScanType function returns the correct reflection type for each
// sql type. The sql type in turn comes from a table column's type.
func TestColumnTypeDatabaseTypeName(t *testing.T) {
	r := sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "field1",
				Type: sqltypes.Int8,
			},
			{
				Name: "field2",
				Type: sqltypes.Uint8,
			},
			{
				Name: "field3",
				Type: sqltypes.Int16,
			},
			{
				Name: "field4",
				Type: sqltypes.Uint16,
			},
			{
				Name: "field5",
				Type: sqltypes.Int24,
			},
			{
				Name: "field6",
				Type: sqltypes.Uint24,
			},
			{
				Name: "field7",
				Type: sqltypes.Int32,
			},
			{
				Name: "field8",
				Type: sqltypes.Uint32,
			},
			{
				Name: "field9",
				Type: sqltypes.Int64,
			},
			{
				Name: "field10",
				Type: sqltypes.Uint64,
			},
			{
				Name: "field11",
				Type: sqltypes.Float32,
			},
			{
				Name: "field12",
				Type: sqltypes.Float64,
			},
			{
				Name: "field13",
				Type: sqltypes.VarBinary,
			},
			{
				Name: "field14",
				Type: sqltypes.Datetime,
			},
		},
	}

	ri := newRows(&r, &converter{}).(driver.RowsColumnTypeDatabaseTypeName)
	defer ri.Close()

	wantTypes := []string{
		"TINYINT",
		"UNSIGNED TINYINT",
		"SMALLINT",
		"UNSIGNED SMALLINT",
		"MEDIUMINT",
		"UNSIGNED MEDIUMINT",
		"INT",
		"UNSIGNED INT",
		"BIGINT",
		"UNSIGNED BIGINT",
		"FLOAT",
		"DOUBLE",
		"VARBINARY",
		"DATETIME",
	}

	for i := range wantTypes {
		require.Equal(t, wantTypes[i], ri.ColumnTypeDatabaseTypeName(i))
	}
}

// Test that the ColumnTypeScanType function returns the correct reflection type for each
// sql type. The sql type in turn comes from a table column's type.
func TestColumnTypeNullable(t *testing.T) {
	r := sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name:  "field1",
				Type:  sqltypes.Int64,
				Flags: uint32(querypb.MySqlFlag_NOT_NULL_FLAG),
			},
			{
				Name: "field2",
				Type: sqltypes.Int64,
			},
		},
	}

	ri := newRows(&r, &converter{}).(driver.RowsColumnTypeNullable)
	defer ri.Close()

	nullable := []bool{
		false,
		true,
	}

	for i := range nullable {
		null, _ := ri.ColumnTypeNullable(i)
		require.Equal(t, nullable[i], null)
	}
}

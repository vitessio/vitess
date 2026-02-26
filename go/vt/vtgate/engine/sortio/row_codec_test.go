/*
Copyright 2025 The Vitess Authors.

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

package sortio

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestRowCodecRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		fields []*querypb.Field
		row    sqltypes.Row
	}{
		{
			name:   "empty row",
			fields: nil,
			row:    sqltypes.Row{},
		},
		{
			name:   "single NULL",
			fields: []*querypb.Field{{Name: "c", Type: querypb.Type_INT64}},
			row:    sqltypes.Row{sqltypes.NULL},
		},
		{
			name: "all NULLs",
			fields: []*querypb.Field{
				{Name: "a", Type: querypb.Type_INT64},
				{Name: "b", Type: querypb.Type_VARCHAR},
				{Name: "c", Type: querypb.Type_VARBINARY},
			},
			row: sqltypes.Row{sqltypes.NULL, sqltypes.NULL, sqltypes.NULL},
		},
		{
			name:   "Int64",
			fields: []*querypb.Field{{Name: "id", Type: querypb.Type_INT64}},
			row:    sqltypes.Row{sqltypes.NewInt64(42)},
		},
		{
			name:   "VarChar",
			fields: []*querypb.Field{{Name: "name", Type: querypb.Type_VARCHAR}},
			row:    sqltypes.Row{sqltypes.NewVarChar("hello world")},
		},
		{
			name:   "VarBinary",
			fields: []*querypb.Field{{Name: "data", Type: querypb.Type_VARBINARY}},
			row:    sqltypes.Row{sqltypes.MakeTrusted(querypb.Type_VARBINARY, []byte{0x00, 0xFF, 0x01})},
		},
		{
			name: "mixed types",
			fields: []*querypb.Field{
				{Name: "a", Type: querypb.Type_INT64},
				{Name: "b", Type: querypb.Type_VARCHAR},
				{Name: "c", Type: querypb.Type_VARCHAR},
				{Name: "d", Type: querypb.Type_FLOAT64},
				{Name: "e", Type: querypb.Type_DATETIME},
			},
			row: sqltypes.Row{
				sqltypes.NewInt64(-100),
				sqltypes.NULL,
				sqltypes.NewVarChar("test string"),
				sqltypes.NewFloat64(3.14),
				sqltypes.MakeTrusted(querypb.Type_DATETIME, []byte("2024-01-15 10:30:00")),
			},
		},
		{
			name:   "Decimal",
			fields: []*querypb.Field{{Name: "d", Type: querypb.Type_DECIMAL}},
			row:    sqltypes.Row{sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("123456789.123456789"))},
		},
		{
			name:   "large value",
			fields: []*querypb.Field{{Name: "big", Type: querypb.Type_VARCHAR}},
			row:    sqltypes.Row{sqltypes.NewVarChar(strings.Repeat("x", 100000))},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codec := NewRowCodec(tt.fields)
			var buf bytes.Buffer
			codec.Encode(&buf, tt.row)

			decoded, err := codec.Decode(&buf)
			require.NoError(t, err)
			require.Len(t, decoded, len(tt.fields))

			for i := range tt.fields {
				if i < len(tt.row) {
					assert.Equal(t, tt.row[i].IsNull(), decoded[i].IsNull(), "column %d null mismatch", i)
					if !tt.row[i].IsNull() {
						assert.Equal(t, tt.row[i].Type(), decoded[i].Type(), "column %d type mismatch", i)
						assert.Equal(t, tt.row[i].Raw(), decoded[i].Raw(), "column %d value mismatch", i)
					}
				} else {
					assert.True(t, decoded[i].IsNull(), "column %d should be NULL", i)
				}
			}
		})
	}
}

func TestRowCodecMultipleRows(t *testing.T) {
	fields := []*querypb.Field{
		{Name: "id", Type: querypb.Type_INT64},
		{Name: "name", Type: querypb.Type_VARCHAR},
	}
	codec := NewRowCodec(fields)
	rows := []sqltypes.Row{
		{sqltypes.NewInt64(1), sqltypes.NewVarChar("first")},
		{sqltypes.NewInt64(2), sqltypes.NewVarChar("second")},
		{sqltypes.NewInt64(3), sqltypes.NULL},
	}

	var buf bytes.Buffer
	for _, row := range rows {
		codec.Encode(&buf, row)
	}

	for i, expected := range rows {
		decoded, err := codec.Decode(&buf)
		require.NoError(t, err, "decoding row %d", i)
		require.Len(t, decoded, 2)
		assert.Equal(t, expected[0].Type(), decoded[0].Type())
		if !expected[1].IsNull() {
			assert.Equal(t, expected[1].Type(), decoded[1].Type())
		}
	}
}

func TestRowCodecEncodedSize(t *testing.T) {
	fields := []*querypb.Field{
		{Name: "id", Type: querypb.Type_INT64},
		{Name: "name", Type: querypb.Type_VARCHAR},
		{Name: "data", Type: querypb.Type_VARCHAR},
	}
	codec := NewRowCodec(fields)
	row := sqltypes.Row{
		sqltypes.NewInt64(42),
		sqltypes.NULL,
		sqltypes.NewVarChar("hello"),
	}

	var buf bytes.Buffer
	codec.Encode(&buf, row)
	assert.Equal(t, codec.EncodedSize(row), buf.Len())
}

func TestRowCodecDecodeEOF(t *testing.T) {
	fields := []*querypb.Field{{Name: "c", Type: querypb.Type_INT64}}
	codec := NewRowCodec(fields)
	var buf bytes.Buffer
	_, err := codec.Decode(&buf)
	assert.Error(t, err)
}

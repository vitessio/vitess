// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"bytes"
	"testing"

	"github.com/youtube/vitess/go/bson"
	"github.com/youtube/vitess/go/sqltypes"
)

type reflectField struct {
	Name string
	Type int64
}

type badQueryResult struct {
	Extra        int
	Fields       []reflectField
	RowsAffected uint64
	InsertId     uint64
	Rows         [][]sqltypes.Value
}

func TestQueryResult(t *testing.T) {
	want := "\x85\x00\x00\x00\x04Fields\x00*\x00\x00\x00\x030\x00\"\x00\x00\x00\x05Name\x00\x04\x00\x00\x00\x00name\x12Type\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x12RowsAffected\x00\x02\x00\x00\x00\x00\x00\x00\x00\x12InsertId\x00\x03\x00\x00\x00\x00\x00\x00\x00\x04Rows\x00 \x00\x00\x00\x040\x00\x18\x00\x00\x00\x050\x00\x01\x00\x00\x00\x001\x051\x00\x02\x00\x00\x00\x00aa\x00\x00\x00"
	custom := QueryResult{
		Fields:       []Field{{"name", 1}},
		RowsAffected: 2,
		InsertId:     3,
		Rows: [][]sqltypes.Value{
			{{sqltypes.Numeric("1")}, {sqltypes.String("aa")}},
		},
	}
	encoded, err := bson.Marshal(&custom)
	if err != nil {
		t.Error(err)
	}
	got := string(encoded)
	if want != got {
		t.Errorf("want\n%#v, got\n%#v", want, got)
	}

	var unmarshalled QueryResult
	err = bson.Unmarshal(encoded, &unmarshalled)
	if err != nil {
		t.Error(err)
	}
	if custom.RowsAffected != unmarshalled.RowsAffected {
		t.Errorf("want %v, got %#v", custom.RowsAffected, unmarshalled.RowsAffected)
	}
	if custom.InsertId != unmarshalled.InsertId {
		t.Errorf("want %v, got %#v", custom.InsertId, unmarshalled.InsertId)
	}
	if custom.Fields[0].Name != unmarshalled.Fields[0].Name {
		t.Errorf("want %v, got %#v", custom.Fields[0].Name, unmarshalled.Fields[0].Name)
	}
	if custom.Fields[0].Type != unmarshalled.Fields[0].Type {
		t.Errorf("want %v, got %#v", custom.Fields[0].Type, unmarshalled.Fields[0].Type)
	}
	if !bytes.Equal(custom.Rows[0][0].Raw(), unmarshalled.Rows[0][0].Raw()) {
		t.Errorf("want %s, got %s", custom.Rows[0][0].Raw(), unmarshalled.Rows[0][0].Raw())
	}
	if !bytes.Equal(custom.Rows[0][1].Raw(), unmarshalled.Rows[0][1].Raw()) {
		t.Errorf("want %s, got %s", custom.Rows[0][0].Raw(), unmarshalled.Rows[0][0].Raw())
	}

	unexpected, err := bson.Marshal(&badQueryResult{})
	if err != nil {
		t.Error(err)
	}
	err = bson.Unmarshal(unexpected, &unmarshalled)
	want = "Unrecognized tag Extra"
	if err == nil || want != err.Error() {
		t.Errorf("want %v, got %v", want, err)
	}
}

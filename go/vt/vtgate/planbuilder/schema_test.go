// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"testing"
)

var (
	idx1 = map[string]IndexFormal{
		"idx1": {},
	}
	idx2 = map[string]IndexFormal{
		"idx2": {
			Type: Lookup,
		},
	}
	t1 = map[string]TableFormal{
		"t1": {
			IndexColumns: []IndexColumnFormal{
				{IndexName: "idx1"},
			},
		},
	}
	t2idx1 = map[string]TableFormal{
		"t2": {
			IndexColumns: []IndexColumnFormal{
				{IndexName: "idx1"},
			},
		},
	}
	t1idx2 = map[string]TableFormal{
		"t1": {
			IndexColumns: []IndexColumnFormal{
				{IndexName: "idx2"},
			},
		},
	}
)

func TestBuildSchemaErrors(t *testing.T) {
	badschema := SchemaFormal{
		Keyspaces: map[string]KeyspaceFormal{
			"ks1": {
				Indexes: idx1,
				Tables:  t1,
			},
			"ks2": {
				Indexes: idx1,
				Tables:  t2idx1,
			},
		},
	}
	_, err := BuildSchema(&badschema)
	want := "index idx1 used in more than one keyspace: ks1 ks2"
	wantother := "index idx1 used in more than one keyspace: ks2 ks1"
	if err == nil || (err.Error() != want && err.Error() != wantother) {
		t.Errorf("got %v, want %s", err, want)
	}

	badschema.Keyspaces["ks2"] = KeyspaceFormal{
		Indexes: idx1,
		Tables:  t1,
	}
	_, err = BuildSchema(&badschema)
	want = "table t1 has multiple definitions"
	if err == nil || err.Error() != want {
		t.Errorf("got %v, want %s", err, want)
	}

	badschema.Keyspaces["ks2"] = KeyspaceFormal{
		Indexes: idx1,
		Tables:  t1,
	}
	_, err = BuildSchema(&badschema)
	want = "table t1 has multiple definitions"
	if err == nil || err.Error() != want {
		t.Errorf("got %v, want %s", err, want)
	}

	badschema = SchemaFormal{
		Keyspaces: map[string]KeyspaceFormal{
			"ks1": {
				Indexes: idx2,
				Tables:  t1,
			},
		},
	}
	_, err = BuildSchema(&badschema)
	want = "index idx1 not found for table t1"
	if err == nil || err.Error() != want {
		t.Errorf("got %v, want %s", err, want)
	}

	badschema = SchemaFormal{
		Keyspaces: map[string]KeyspaceFormal{
			"ks1": {
				Indexes: idx2,
				Tables:  t1idx2,
			},
		},
	}
	_, err = BuildSchema(&badschema)
	want = "first index is not ShardKey for table t1"
	if err == nil || err.Error() != want {
		t.Errorf("got %v, want %s", err, want)
	}
}

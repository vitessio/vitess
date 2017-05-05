/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqlannotation

import (
	"reflect"
	"strings"
	"testing"
)

func TestExtractKeyspaceIDKeyspaceID(t *testing.T) {
	keyspaceIDS, err := ExtractKeyspaceIDS("DML /* vtgate:: keyspace_id:25AF,25BF */")
	if err != nil {
		t.Errorf("want nil, got: %v", err)
	}
	if !reflect.DeepEqual(keyspaceIDS[0], []byte{0x25, 0xAF}) {
		t.Errorf("want: %v, got: %v", []byte{0x25, 0xAF}, keyspaceIDS[0])
	}

	if !reflect.DeepEqual(keyspaceIDS[1], []byte{0x25, 0xBF}) {
		t.Errorf("want: %v, got: %v", []byte{0x25, 0xBF}, keyspaceIDS[1])
	}
}

func TestExtractKeyspaceIDUnfriendly(t *testing.T) {
	_, err := ExtractKeyspaceIDS("DML /* vtgate:: filtered_replication_unfriendly */")
	extErr, ok := err.(*ExtractKeySpaceIDError)
	if !ok {
		t.Fatalf("want a type *ExtractKeySpaceIDError, got: %v", err)
	}
	if extErr.Kind != ExtractKeySpaceIDReplicationUnfriendlyError {
		t.Errorf("want ExtractKeySpaceIDReplicationUnfriendlyError got: %v", err)
	}
}

func TestExtractKeyspaceIDParseError(t *testing.T) {
	verifyParseError(t, "DML /* vtgate:: filtered_replication_unfriendly */  /* vtgate:: keyspace_id:25AF */")
	verifyParseError(t, "DML /* vtgate:: filtered_replication_unfriendl")
	verifyParseError(t, "DML /* vtgate:: keyspace_id:25A */")
	verifyParseError(t, "DML /* vtgate:: keyspace_id:25AFG */")
	verifyParseError(t, "DML")
}

func verifyParseError(t *testing.T, sql string) {
	_, err := ExtractKeyspaceIDS(sql)
	extErr, ok := err.(*ExtractKeySpaceIDError)
	if !ok {
		t.Fatalf("want a type *ExtractKeySpaceIDError, got: %v", err)
	}
	if extErr.Kind != ExtractKeySpaceIDParseError {
		t.Errorf("want ExtractKeySpaceIDParseError got: %v", err)
	}
}

func BenchmarkExtractKeyspaceIDKeyspaceID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ExtractKeyspaceIDS("DML /* vtgate:: keyspace_id:25AF */")
	}
}

func BenchmarkNativeExtractKeyspaceIDKeyspaceID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ExtractKeyspaceIDS("DML /* vtgate:: keyspace_id:25AF */")
	}
}

func BenchmarkExtractKeySpaceIDReplicationUnfriendly(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ExtractKeyspaceIDS("DML /* vtgate:: filtered_replication_unfriendly */")
	}
}

func BenchmarkExtractKeySpaceIDNothing(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ExtractKeyspaceIDS("DML")
	}
}

func TestAddKeyspaceIDs(t *testing.T) {
	ksid1 := []byte{0x37}
	ksid2 := []byte{0x29}
	ksids := make([][]byte, 0)
	ksids = append(ksids, ksid1)
	ksids = append(ksids, ksid2)

	sql := AddKeyspaceIDs("DML", [][]byte{ksid1}, "trailing_comments")
	if !strings.EqualFold(sql, "DML /* vtgate:: keyspace_id:37 */trailing_comments") {
		t.Errorf("want: %s, got: %s", "DML /* vtgate:: keyspace_id:37 */trailing_comments", sql)
	}

	sql = AddKeyspaceIDs("DML", ksids, "trailing_comments")
	if !strings.EqualFold(sql, "DML /* vtgate:: keyspace_id:37,29 */trailing_comments") {
		t.Errorf("want: %s, got: %s", "DML /* vtgate:: keyspace_id:37,29 */trailing_comments", sql)
	}
}

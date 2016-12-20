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

var IsDMLTests = []struct {
	sql   string
	isDML bool
}{
	{"   update ...", true},
	{"UPDATE ...", true},
	{"\n\t    delete ...", true},
	{"insert ...", true},
	{"select ...", false},
	{"    select ...", false},
	{"", false},
	{" ", false},
}

func TestIsDML(t *testing.T) {
	for _, test := range IsDMLTests {
		if dml := IsDML(test.sql); dml != test.isDML {
			t.Errorf("IsDML(%q) = %t, got %t", test.sql, dml, test.isDML)
		}
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

func BenchmarkIsDML(b *testing.B) {
	for i := 0; i < b.N; i++ {
		IsDML("UPDATE ultimatequestion set answer=42 where question = 'What do you get if you multiply six by nine?'")
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

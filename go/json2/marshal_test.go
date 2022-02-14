package json2

import (
	"testing"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

func TestMarshalPB(t *testing.T) {
	col := &vschemapb.Column{
		Name: "c1",
		Type: querypb.Type_VARCHAR,
	}
	b, err := MarshalPB(col)
	if err != nil {
		t.Fatal(err)
	}
	want := "{\"name\":\"c1\",\"type\":\"VARCHAR\"}"
	if string(b) != want {
		t.Errorf("MarshalPB(col): %q, want %q", b, want)
	}
}

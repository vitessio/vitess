package vindexes

import (
	"reflect"
	"testing"
)

var varcharHash Vindex

func init() {
	varcharHash, _ = CreateVindex("varcharHash", "vch", nil)
}

func TestVarcharHashCost(t *testing.T) {
	if varcharHash.Cost() != 1 {
		t.Errorf("Cost(): %d, want 1", varcharHash.Cost())
	}
}

//TestVarcharMap checks if the [upper/lower/mixed]case strings return the same hash
func TestVarcharMap(t *testing.T) {
	got, err := varcharHash.(Unique).Map(nil, []interface{}{"TEST", "test", "TeSt"})
	if err != nil {
		t.Error(err)
	}
	want := [][]byte{
		[]byte("\xea\x0a\x5a\x2b\x5e\xae\xac\x89"),
		[]byte("\xea\x0a\x5a\x2b\x5e\xae\xac\x89"),
		[]byte("\xea\x0a\x5a\x2b\x5e\xae\xac\x89"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}
}

func TestVarCharVerify(t *testing.T) {
	success, err := varcharHash.Verify(nil, "TeSt", []byte("\xea\x0a\x5a\x2b\x5e\xae\xac\x89"))
	if err != nil {
		t.Error(err)
	}
	if !success {
		t.Errorf("Verify(): %+v, want true", success)
	}
}

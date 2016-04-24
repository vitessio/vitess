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

func TestVarcharMap(t *testing.T) {
	/*const sample  = "TEST"
	for i := 0; i < len(sample); i++ {
		fmt.Printf("%x ", sample[i])
	}*/
	got, err := varcharHash.(Unique).Map(nil, []interface{}{"TEST", "test", "TeSt"})
	if err != nil {
		t.Error(err)
	}
	want := [][]byte{
		[]byte("\x07\x0c\x00\x04\x74\x65\x73\x74"),
		[]byte("\x07\x0c\x00\x04\x74\x65\x73\x74"),
		[]byte("\x07\x0c\x00\x04\x74\x65\x73\x74"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}
}

func TestVarCharVerify(t *testing.T) {
	success, err := varcharHash.Verify(nil, "TeSt", []byte("\x07\x0c\x00\x04\x74\x65\x73\x74"))
	if err != nil {
		t.Error(err)
	}
	if !success {
		t.Errorf("Verify(): %+v, want true", success)
	}
}

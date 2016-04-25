package vindexes

import (
	"reflect"
	"testing"
)

var utf8cihash Vindex

func init() {
	utf8cihash, _ = CreateVindex("utf8cihash", "utf8ch", nil)
}

func TestVarcharHashCost(t *testing.T) {
	if utf8cihash.Cost() != 1 {
		t.Errorf("Cost(): %d, want 1", utf8cihash.Cost())
	}
}

//TestVarcharMap checks if the [upper/lower/mixed]case strings return the same hash
//eg: TESTTEST, testtest,TeStteST
func TestVarcharMap(t *testing.T) {
	got, err := utf8cihash.(Unique).Map(nil, []interface{}{[]byte("\x55\x45\x54\x55\x55\x45\x54\x55"), []byte("\x75\x65\x74\x75\x75\x65\x74\x75"),
		[]byte("\x55\x65\x54\x75\x75\x65\x54\x55")})
	if err != nil {
		t.Error(err)
	}
	want := [][]byte{
		[]byte("\xf7\xaa\x9a\x46\xc9\x20\x85\x65"),
		[]byte("\xf7\xaa\x9a\x46\xc9\x20\x85\x65"),
		[]byte("\xf7\xaa\x9a\x46\xc9\x20\x85\x65"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}
}

func TestVarCharVerify(t *testing.T) {
	success, err := utf8cihash.Verify(nil, []byte("\x55\x45\x54\x55\x55\x45\x54\x55"), []byte("\xf7\xaa\x9a\x46\xc9\x20\x85\x65"))
	if err != nil {
		t.Error(err)
	}
	if !success {
		t.Errorf("Verify(): %+v, want true", success)
	}
}

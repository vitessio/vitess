package vindexes

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
)

var lookupUnique Vindex
var lookupNonUnique Vindex

func init() {
	lunique, err := CreateVindex("lookup_unique", "lookupUnique", map[string]string{"table": "t", "from": "fromc", "to": "toc"})
	if err != nil {
		panic(err)
	}
	lnonunique, err := CreateVindex("lookup", "lookupNonUnique", map[string]string{"table": "t", "from": "fromc", "to": "toc"})
	lookupUnique = lunique
	lookupNonUnique = lnonunique
}

func TestLookupUniqueCost(t *testing.T) {
	if lookupUnique.Cost() != 10 {
		t.Errorf("Cost(): %d, want 10", lookupUnique.Cost())
	}
}

func TestLookupNonUniqueCost(t *testing.T) {
	if lookupNonUnique.Cost() != 20 {
		t.Errorf("Cost(): %d, want 20", lookupUnique.Cost())
	}
}

func TestLookupUniqueVerify(t *testing.T) {
	vc := &vcursor{numRows: 1}
	success, err := lookupUnique.Verify(vc, 1, []byte("test"))
	if err != nil {
		t.Error(err)
	}
	if !success {
		t.Errorf("Verify(): %+v, want true", success)
	}
}

func TestLookupUniqueCreate(t *testing.T) {
	vc := &vcursor{}
	err := lookupUnique.(Lookup).Create(vc, 1, []byte("test"))
	if err != nil {
		t.Error(err)
	}
	wantQuery := &querytypes.BoundQuery{
		Sql: "insert into t(fromc, toc) values(:fromc, :toc)",
		BindVariables: map[string]interface{}{
			"fromc": 1,
			"toc":   []byte("test"),
		},
	}
	if !reflect.DeepEqual(vc.bq, wantQuery) {
		t.Errorf("vc.query = %#v, want %#v", vc.bq, wantQuery)
	}
}

func TestLookupUniqueReverse(t *testing.T) {
	_, ok := lookupUnique.(Reversible)
	if ok {
		t.Errorf("lhu.(Reversible): true, want false")
	}
}

func TestLookupUniqueDelete(t *testing.T) {
	vc := &vcursor{}
	err := lookupUnique.(Lookup).Delete(vc, []interface{}{1}, []byte("test"))
	if err != nil {
		t.Error(err)
	}
	wantQuery := &querytypes.BoundQuery{
		Sql: "delete from t where fromc = :fromc and toc = :toc",
		BindVariables: map[string]interface{}{
			"fromc": 1,
			"toc":   []byte("test"),
		},
	}
	if !reflect.DeepEqual(vc.bq, wantQuery) {
		t.Errorf("vc.query = %#v, want %#v", vc.bq, wantQuery)
	}
}

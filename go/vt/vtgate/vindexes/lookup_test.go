package vindexes

import (
	"reflect"
	"testing"

	"strings"

	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/querytypes"
)

var lookupUnique Vindex
var lookupNonUnique Vindex

func init() {
	lkpunique, err := CreateVindex("lookup_unique", "lookupUnique", map[string]string{"table": "t", "from": "fromc", "to": "toc"})
	if err != nil {
		panic(err)
	}
	lkpnonunique, err := CreateVindex("lookup", "lookupNonUnique", map[string]string{"table": "t", "from": "fromc", "to": "toc"})
	if err != nil {
		panic(err)
	}

	lookupUnique = lkpunique
	lookupNonUnique = lkpnonunique
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

func TestLookupUniqueString(t *testing.T) {
	if strings.Compare("lookupUnique", lookupUnique.String()) != 0 {
		t.Errorf("String(): %s, want lookupUnique", lookupUnique.String())
	}
}

func TestLookupNonUniqueString(t *testing.T) {
	if strings.Compare("lookupNonUnique", lookupNonUnique.String()) != 0 {
		t.Errorf("String(): %s, want lookupNonUnique", lookupNonUnique.String())
	}
}

func TestLookupUniqueVerify(t *testing.T) {
	vc := &vcursor{numRows: 1}
	_, err := lookupUnique.Verify(vc, []interface{}{1}, [][]byte{[]byte("test")})
	wantQuery := &querytypes.BoundQuery{
		Sql: "select fromc from t where ((fromc=:fromc0 and toc=:toc0))",
		BindVariables: map[string]interface{}{
			"fromc0": 1,
			"toc0":   []byte("test"),
		},
	}
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(vc.bq, wantQuery) {
		t.Errorf("vc.query = %#v, want %#v", vc.bq, wantQuery)
	}

	//Negative test
	want := "lookup.Verify:length of ids 2 doesn't match length of ksids 1"
	_, err = lookupUnique.Verify(vc, []interface{}{1, 2}, [][]byte{[]byte("test")})
	if err.Error() != want {
		t.Error(err.Error())
	}

	_, err = lookuphashunique.Verify(nil, []interface{}{1}, [][]byte{[]byte("test1test23")})
	want = "lookup.Verify: invalid keyspace id: 7465737431746573743233"
	if err.Error() != want {
		t.Error(err)
	}
}

func TestLookupUniqueMap(t *testing.T) {
	vc := &vcursor{}
	_, err := lookupUnique.(Unique).Map(vc, []interface{}{2})
	if err != nil {
		t.Error(err)
	}
	wantQuery := &querytypes.BoundQuery{
		Sql: "select toc from t where fromc = :fromc",
		BindVariables: map[string]interface{}{
			"fromc": 2,
		},
	}
	if !reflect.DeepEqual(vc.bq, wantQuery) {
		t.Errorf("vc.query = %#v, want %#v", vc.bq, wantQuery)
	}
}

func TestLookupUniqueCreate(t *testing.T) {
	vc := &vcursor{}
	err := lookupUnique.(Lookup).Create(vc, []interface{}{1}, [][]byte{[]byte("test")})
	if err != nil {
		t.Error(err)
	}
	wantQuery := &querytypes.BoundQuery{
		Sql: "insert into t(fromc,toc) values(:fromc0,:toc0)",
		BindVariables: map[string]interface{}{
			"fromc0": 1,
			"toc0":   []byte("test"),
		},
	}
	if !reflect.DeepEqual(vc.bq, wantQuery) {
		t.Errorf("vc.query = %#v, want %#v", vc.bq, wantQuery)
	}

	//Negative test
	want := "lookup.Create:length of ids 2 doesn't match length of ksids 1"
	err = lookupUnique.(Lookup).Create(vc, []interface{}{1, 2}, [][]byte{[]byte("test")})
	if err.Error() != want {
		t.Error(err.Error())
	}

	err = lookuphashunique.(Lookup).Create(nil, []interface{}{1}, [][]byte{[]byte("test1test23")})
	want = "lookup.Create: invalid keyspace id: 7465737431746573743233"
	if err.Error() != want {
		t.Error(err)
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

	//Negative Test
	err = lookuphashunique.(Lookup).Delete(vc, []interface{}{1}, []byte("test1test23"))
	want := "lookup.Delete: invalid keyspace id: 7465737431746573743233"
	if err.Error() != want {
		t.Error(err)
	}
}

func TestLookupNonUniqueVerify(t *testing.T) {
	vc := &vcursor{numRows: 1}
	_, err := lookupNonUnique.Verify(vc, []interface{}{1}, [][]byte{[]byte("test")})
	wantQuery := &querytypes.BoundQuery{
		Sql: "select fromc from t where ((fromc=:fromc0 and toc=:toc0))",
		BindVariables: map[string]interface{}{
			"fromc0": 1,
			"toc0":   []byte("test"),
		},
	}
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(vc.bq, wantQuery) {
		t.Errorf("vc.query = %#v, want %#v", vc.bq, wantQuery)
	}
}

func TestLookupNonUniqueMap(t *testing.T) {
	vc := &vcursor{}
	_, err := lookupNonUnique.(NonUnique).Map(vc, []interface{}{2})
	if err != nil {
		t.Error(err)
	}
	wantQuery := &querytypes.BoundQuery{
		Sql: "select toc from t where fromc = :fromc",
		BindVariables: map[string]interface{}{
			"fromc": 2,
		},
	}
	if !reflect.DeepEqual(vc.bq, wantQuery) {
		t.Errorf("vc.query = %#v, want %#v", vc.bq, wantQuery)
	}
}

func TestLookupNonUniqueCreate(t *testing.T) {
	vc := &vcursor{}
	err := lookupNonUnique.(Lookup).Create(vc, []interface{}{1}, [][]byte{[]byte("test")})
	if err != nil {
		t.Error(err)
	}
	wantQuery := &querytypes.BoundQuery{
		Sql: "insert into t(fromc,toc) values(:fromc0,:toc0)",
		BindVariables: map[string]interface{}{
			"fromc0": 1,
			"toc0":   []byte("test"),
		},
	}
	if !reflect.DeepEqual(vc.bq, wantQuery) {
		t.Errorf("vc.query = %#v, want %#v", vc.bq, wantQuery)
	}
}

func TestLookupNonUniqueReverse(t *testing.T) {
	_, ok := lookupNonUnique.(Reversible)
	if ok {
		t.Errorf("lhu.(Reversible): true, want false")
	}
}

func TestLookupNonUniqueDelete(t *testing.T) {
	vc := &vcursor{}
	err := lookupNonUnique.(Lookup).Delete(vc, []interface{}{1}, []byte("test"))
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

package planbuilder

import (
	"reflect"
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestGenerateSubqueryVars(t *testing.T) {
	reserved := sqlparser.NewReservedVars("vtg", map[string]struct{}{
		"__sq1":            {},
		"__sq_has_values3": {},
	})
	jt := newJointab(reserved)

	v1, v2 := jt.GenerateSubqueryVars()
	combined := []string{v1, v2}
	want := []string{"__sq2", "__sq_has_values2"}
	if !reflect.DeepEqual(combined, want) {
		t.Errorf("jt.GenerateSubqueryVars: %v, want %v", combined, want)
	}

	v1, v2 = jt.GenerateSubqueryVars()
	combined = []string{v1, v2}
	want = []string{"__sq4", "__sq_has_values4"}
	if !reflect.DeepEqual(combined, want) {
		t.Errorf("jt.GenerateSubqueryVars: %v, want %v", combined, want)
	}
}

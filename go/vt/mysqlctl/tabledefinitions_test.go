package mysqlctl

import (
	"sort"
	"testing"

	"vitess.io/vitess/go/test/utils"
)

func TestTableDefinitionsAreSorted(t *testing.T) {
	// assert that two collections of table definitions are comparable even if
	// the inputs are ordered differently
	tds1 := tableDefinitions{{
		Name:   "table1",
		Schema: "schema1",
	}, {
		Name:   "table2",
		Schema: "schema2",
	}}
	tds2 := tableDefinitions{{
		Name:   "table2",
		Schema: "schema2",
	}, {
		Name:   "table1",
		Schema: "schema1",
	}}
	sort.Sort(tds1)
	sort.Sort(tds2)
	utils.MustMatch(t, tds1, tds2, "")
}

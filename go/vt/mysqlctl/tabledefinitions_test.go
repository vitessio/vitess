/*
Copyright 2020 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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

/*
Copyright 2026 The Vitess Authors.

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

package evalengine

import (
	"vitess.io/vitess/go/mysql/collations"
)

// rowTextCollationNames are collations that colldata implements, but whose
// text VTGate does not compare, hash or sort when it comes from a tablet.
//
// A tablet sends the text of every column in utf8mb4, and these collations
// read the bytes of another character set: latin1_german2_ci would take 'ä',
// which arrives as C3 A4, for the latin1 text 'Ã¤', and sort it after 'af'.
// For a column in one of them, the planner asks the shards for its
// weight_string(), and VTGate orders, groups and hashes rows by that, as it did
// before Vitess implemented these collations. Comparing the text itself needs
// VTGate to convert it from utf8mb4 to the character set of the collation
// first.
//
// The collations of these character sets that Vitess implemented earlier, such
// as latin1_swedish_ci, are not here: VTGate compares their text as it
// arrives, as it has done before.
var rowTextCollationNames = []string{
	"latin1_german2_ci",
	"latin2_czech_cs",
	"cp1250_czech_cs",
	"tis620_thai_ci",
	"tis620_bin",
	"gbk_chinese_ci",
	"gbk_bin",
	"big5_chinese_ci",
	"big5_bin",
	"gb18030_chinese_ci",
	"gb18030_bin",
	"ucs2_general_mysql500_ci",
}

var rowTextCollations = func() map[collations.ID]struct{} {
	env := collations.MySQL8()
	ids := make(map[collations.ID]struct{}, len(rowTextCollationNames))
	for _, name := range rowTextCollationNames {
		id := env.LookupByName(name)
		if id == collations.Unknown {
			panic("evalengine: unknown collation " + name)
		}
		ids[id] = struct{}{}
	}
	return ids
}()

// NeedsServerWeightString reports whether VTGate compares, hashes and sorts
// the text of rows in collation by the weight_string() that MySQL computes for
// it, and not by the text itself, see rowTextCollationNames.
func NeedsServerWeightString(collation collations.ID) bool {
	_, ok := rowTextCollations[collation]
	return ok
}

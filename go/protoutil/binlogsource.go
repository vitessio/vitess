/*
Copyright 2024 The Vitess Authors.

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

package protoutil

import (
	"slices"
	"sort"
	"strings"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

// SortBinlogSourceTables sorts the table related contents of the
// BinlogSource struct lexicographically by table name in order to
// produce consistent results.
func SortBinlogSourceTables(bls *binlogdatapb.BinlogSource) {
	if bls == nil {
		return
	}

	// Sort the tables by name to ensure a consistent order.
	slices.Sort(bls.Tables)

	if bls.Filter == nil || len(bls.Filter.Rules) == 0 {
		return
	}
	sort.Slice(bls.Filter.Rules, func(i, j int) bool {
		// Exclude filters should logically be processed first.
		if bls.Filter.Rules[i].Filter == "exclude" && bls.Filter.Rules[j].Filter != "exclude" {
			return true
		}
		if bls.Filter.Rules[j].Filter == "exclude" && bls.Filter.Rules[i].Filter != "exclude" {
			return false
		}

		// Remove preceding slash from the match string.
		// That is used when the filter is a regular expression.
		fi, _ := strings.CutPrefix(bls.Filter.Rules[i].Match, "/")
		fj, _ := strings.CutPrefix(bls.Filter.Rules[j].Match, "/")
		if fi != fj {
			return fi < fj
		}

		return bls.Filter.Rules[i].Filter < bls.Filter.Rules[j].Filter
	})
}

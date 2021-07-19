/*
Copyright 2019 The Vitess Authors.

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

package topoproto

import (
	"fmt"
	"testing"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestParseTabletAlias(t *testing.T) {
	for aliasStr, expectedAlias := range map[string]*topodatapb.TabletAlias{
		// valid cases
		"cell1-42":                         {Cell: "cell1", Uid: 42},
		"cell1_1-42":                       {Cell: "cell1_1", Uid: 42},
		"cell1_1-0":                        {Cell: "cell1_1", Uid: 0},
		"cell1_1--1":                       {Cell: "cell1_1-", Uid: 1},
		"1-022222":                         {Cell: "1", Uid: 22222},
		"global-read-only-000000000000042": {Cell: "global-read-only", Uid: 42},
		"-cell1-1---42":                    {Cell: "-cell1-1--", Uid: 42},
		"cell1____-42":                     {Cell: "cell1____", Uid: 42},
		"__cell1-1-1-2-42":                 {Cell: "__cell1-1-1-2", Uid: 42},

		// invalid cases
		"":          nil,
		"42":        nil,
		"-42":       nil,
		"cell1":     nil,
		"cell1-":    nil,
		"cell1_42":  nil,
		",cell1-42": nil,
	} {
		alias, err := ParseTabletAlias(aliasStr)

		if expectedAlias == nil {
			if err == nil {
				t.Fatalf("Expected to fail parsing invalid tablet alias: %s but got no error", aliasStr)
			} else {
				expectedErr := fmt.Errorf("invalid tablet alias: '%s', expecting format: '%s'", aliasStr, tabletAliasFormat)
				if err.Error() != expectedErr.Error() {
					t.Fatalf("Expected error: %s but got: %s", expectedErr, err)
				}
				continue
			}
		}

		if err != nil {
			t.Fatalf("Failed to parse valid tablet alias: %s, err: %s", aliasStr, err)
		}
		if alias.Cell != expectedAlias.Cell {
			t.Fatalf("Cell parsed from tabletAlias: %s is %s but expected %s", aliasStr, alias.Cell, expectedAlias.Cell)
		}
		if alias.Uid != expectedAlias.Uid {
			t.Fatalf("Uid parsed from tabletAlias: %s is %d but expected %d", aliasStr, alias.Uid, expectedAlias.Uid)
		}
	}
}

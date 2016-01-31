// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

func TestFilter(t *testing.T) {
	schema, err := LoadFile(locateFile("schema_test.json"))
	if err != nil {
		t.Fatal(err)
	}
	for tcase := range iterateExecFile("filter_cases.txt") {
		statement, err := sqlparser.Parse(tcase.input)
		if err != nil {
			t.Error(err)
			continue
		}
		sel, ok := statement.(*sqlparser.Select)
		if !ok {
			t.Errorf("unexpected type: %T", statement)
			continue
		}
		plan, _, err := buildSelectPlan2(sel, schema)
		if err != nil {
			t.Error(err)
			continue
		}
		bout, err := json.Marshal(plan)
		if err != nil {
			panic(fmt.Sprintf("Error marshalling %v: %v", plan, err))
		}
		out := string(bout)
		if out != tcase.output {
			t.Errorf("Line:%v\n%s\n%s", tcase.lineno, tcase.output, out)
		}
		// Comment these line out to see the expected outputs
		// bout, err = json.MarshalIndent(plan, "", "  ")
		// fmt.Printf("%s\n", bout)
	}
}

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

package wrangler

import (
	"fmt"
	"testing"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

func TestVDiffPlan(t *testing.T) {
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: fmt.Sprintf("select * from t1 where in_keyrange('-80')"),
		}},
	}

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}
	differs, err := buildVDiffPlan(context.Background(), filter, schm)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%+v\n", differs["t1"])
}

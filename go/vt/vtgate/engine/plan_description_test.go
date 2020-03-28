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

package engine

import (
	"fmt"
	"testing"

	"vitess.io/vitess/go/sqltypes"

	"github.com/google/go-cmp/cmp"
	"vitess.io/vitess/go/jsonutil"
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestCreatePlanDescription(t *testing.T) {
	del := &Route{
		Opcode:            SelectScatter,
		Keyspace:          &vindexes.Keyspace{Name: "ks"},
		TargetDestination: key.DestinationAllShards{},
		TargetTabletType:  topodatapb.TabletType_MASTER,
		Query:             "select all the things",
		TableName:         "tableName",
		FieldQuery:        "more query",
		Vindex:            &vindexes.Null{},
		Values:            []sqltypes.PlanValue{},
		OrderBy:           []OrderbyParams{},
	}

	planDescription := PrimitiveToPlanDescription(del)

	expected := PlanDescription{
		OperatorType: "Route",
		OpCode:       "SelectScatter",
		Keyspace:     "ks",
		Destination:  "DestinationAllShards()",
		TabletType:   topodatapb.TabletType_MASTER,
	}

	if diff := cmp.Diff(planDescription, expected); diff != "" {
		t.Errorf(diff)
		bytes, _ := jsonutil.MarshalNoEscape(expected)
		fmt.Println(string(bytes))
		fmt.Printf("%v\n", expected)
	}

}

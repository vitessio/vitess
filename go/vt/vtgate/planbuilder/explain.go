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

package planbuilder

import (
	"encoding/json"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// Builds an explain-plan for the given Primitive
func buildExplainPlan(input engine.Primitive) (engine.Primitive, error) {
	description := engine.PrimitiveToPlanDescription(input)

	indent := "  "
	prefix := ""
	jsonByt, err := json.MarshalIndent(description, prefix, indent)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(jsonByt), "/n")
	var rows [][]sqltypes.Value
	for _, line := range lines {
		rows = append(rows, []sqltypes.Value{sqltypes.NewVarChar(line)})
	}

	fields := []*querypb.Field{{
		Name: "description",
		Type: querypb.Type_VARCHAR,
	}}

	return engine.NewRowsPrimitive(rows, fields), nil
}

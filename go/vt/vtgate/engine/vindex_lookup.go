/*
Copyright 2022 The Vitess Authors.

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
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

var _ Primitive = (*VindexLookup)(nil)

type VindexLookup struct {
	*RoutingParameters

	// If we need to fetch data in order to do the map, this is where we get it
	Lookup Primitive

	// This is the side that needs to be routed
	SendTo Primitive
}

// RouteType implements the Primitive interface
func (vr *VindexLookup) RouteType() string {
	return vr.Opcode.String()
}

// GetKeyspaceName implements the Primitive interface
func (vr *VindexLookup) GetKeyspaceName() string {
	return vr.Keyspace.Name
}

// GetTableName implements the Primitive interface
func (vr *VindexLookup) GetTableName() string {
	return vr.SendTo.GetTableName()
}

// GetFields implements the Primitive interface
func (vr *VindexLookup) GetFields(vcursor VCursor, routing *RouteDestination, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	panic("implement me")
}

// NeedsTransaction implements the Primitive interface
func (vr *VindexLookup) NeedsTransaction() bool {
	return vr.SendTo.NeedsTransaction()
}

// TryExecute implements the Primitive interface
func (vr *VindexLookup) TryExecute(vcursor VCursor, inRoute *RouteDestination, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	if inRoute != nil {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "does not accept routing")
	}
	var out []key.Destination
	var outBindVars []map[string]*querypb.BindVariable
	if vr.Lookup != nil {
		res, err := vcursor.ExecutePrimitive(vr.Lookup, nil, bindVars, wantfields)
		if err != nil {
			return nil, err
		}

		ksids := make([][]byte, 0, len(res.Rows))
		for _, row := range res.Rows {
			rowBytes, err := row[0].ToBytes()
			if err != nil {
				return nil, err
			}
			ksids = append(ksids, rowBytes)
		}
		out = append(out, key.DestinationKeyspaceIDs(ksids))
		outBindVars = append(outBindVars, bindVars)
	}
	env := evalengine.EnvWithBindVars(bindVars, vcursor.ConnCollation())

	var values []*querypb.Value
	for _, expr := range vr.Values {
		evaluate, err := env.Evaluate(expr)
		if err != nil {
			return nil, err
		}
		values = append(values, sqltypes.ValueToProto(evaluate.Value()))
	}

	destinations, _, err := vcursor.ResolveDestinations(vr.Keyspace.Name, values, out)
	if err != nil {
		return nil, err
	}

	routing := &RouteDestination{
		Shards:   destinations,
		BindVars: outBindVars,
	}
	return vcursor.ExecutePrimitive(vr.SendTo, routing, bindVars, wantfields)
}

// TryStreamExecute implements the Primitive interface
func (vr *VindexLookup) TryStreamExecute(vcursor VCursor, routing *RouteDestination, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	//TODO implement me
	panic("implement me")
}

// Inputs implements the Primitive interface
func (vr *VindexLookup) Inputs() []Primitive {
	if vr.Lookup != nil {
		return []Primitive{vr.Lookup, vr.SendTo}
	}

	return []Primitive{vr.SendTo}
}

// description implements the Primitive interface
func (vr *VindexLookup) description() PrimitiveDescription {
	other := map[string]any{}
	if vr.Values != nil {
		formattedValues := make([]string, 0, len(vr.Values))
		for _, value := range vr.Values {
			formattedValues = append(formattedValues, evalengine.FormatExpr(value))
		}
		other["Values"] = formattedValues
	}

	return PrimitiveDescription{
		OperatorType: "VindexLookup",
		Variant:      vr.Opcode.String(),
		Keyspace:     vr.Keyspace,
		Other:        other,
	}
}

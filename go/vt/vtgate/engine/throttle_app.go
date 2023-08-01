/*
Copyright 2023 The Vitess Authors.

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
	"context"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ Primitive = (*ThrottleApp)(nil)

// ThrottleApp represents the instructions to perform an online schema change via vtctld
type ThrottleApp struct {
	Keyspace         *vindexes.Keyspace
	ThrottledAppRule *topodatapb.ThrottledAppRule

	noTxNeeded

	noInputs
}

func (v *ThrottleApp) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "ThrottleApp",
		Keyspace:     v.Keyspace,
		Other: map[string]any{
			"appName":  v.ThrottledAppRule.Name,
			"expireAt": v.ThrottledAppRule.ExpiresAt,
			"ratio":    v.ThrottledAppRule.Ratio,
		},
	}
}

// RouteType implements the Primitive interface
func (v *ThrottleApp) RouteType() string {
	return "ThrottleApp"
}

// GetKeyspaceName implements the Primitive interface
func (v *ThrottleApp) GetKeyspaceName() string {
	return v.Keyspace.Name
}

// GetTableName implements the Primitive interface
func (v *ThrottleApp) GetTableName() string {
	return ""
}

// TryExecute implements the Primitive interface
func (v *ThrottleApp) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (result *sqltypes.Result, err error) {
	if err := vcursor.ThrottleApp(ctx, v.ThrottledAppRule); err != nil {
		return nil, err
	}
	return &sqltypes.Result{}, nil
}

// TryStreamExecute implements the Primitive interface
func (v *ThrottleApp) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	results, err := v.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(results)
}

// GetFields implements the Primitive interface
func (v *ThrottleApp) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] GetFields is not reachable")
}

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
	"encoding/json"
	"fmt"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/mysql"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	// Set contains the instructions to perform set.
	Set struct {
		Ops   []SetOp
		Input Primitive

		noTxNeeded
	}

	// SetOp is an interface that different type of set operations implements.
	SetOp interface {
		Execute(vcursor VCursor, env evalengine.ExpressionEnv) error
		VariableName() string
	}

	// UserDefinedVariable implements the SetOp interface to execute user defined variables.
	UserDefinedVariable struct {
		Name string
		Expr evalengine.Expr
	}

	// SysVarIgnore implements the SetOp interface to ignore the settings.
	SysVarIgnore struct {
		Name string
		Expr string
	}

	// SysVarCheckAndIgnore implements the SetOp interface to check underlying setting and ignore if same.
	SysVarCheckAndIgnore struct {
		Name              string
		Keyspace          *vindexes.Keyspace
		TargetDestination key.Destination
		Expr              string
	}
)

var _ Primitive = (*Set)(nil)

//RouteType implements the Primitive interface method.
func (s *Set) RouteType() string {
	return "Set"
}

//GetKeyspaceName implements the Primitive interface method.
func (s *Set) GetKeyspaceName() string {
	return ""
}

//GetTableName implements the Primitive interface method.
func (s *Set) GetTableName() string {
	return ""
}

//Execute implements the Primitive interface method.
func (s *Set) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, _ bool) (*sqltypes.Result, error) {
	input, err := s.Input.Execute(vcursor, bindVars, false)
	if err != nil {
		return nil, err
	}
	if len(input.Rows) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "should get a single row")
	}
	env := evalengine.ExpressionEnv{
		BindVars: bindVars,
		Row:      input.Rows[0],
	}
	for _, setOp := range s.Ops {
		err := setOp.Execute(vcursor, env)
		if err != nil {
			return nil, err
		}
	}
	return &sqltypes.Result{}, nil
}

//StreamExecute implements the Primitive interface method.
func (s *Set) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantields bool, callback func(*sqltypes.Result) error) error {
	result, err := s.Execute(vcursor, bindVars, wantields)
	if err != nil {
		return err
	}
	return callback(result)
}

//GetFields implements the Primitive interface method.
func (s *Set) GetFields(VCursor, map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return &sqltypes.Result{}, nil
}

//Inputs implements the Primitive interface
func (s *Set) Inputs() []Primitive {
	return []Primitive{s.Input}
}

func (s *Set) description() PrimitiveDescription {
	other := map[string]interface{}{
		"Ops": s.Ops,
	}
	return PrimitiveDescription{
		OperatorType: "Set",
		Variant:      "",
		Other:        other,
	}
}

var _ SetOp = (*UserDefinedVariable)(nil)

//MarshalJSON provides the type to SetOp for plan json
func (u *UserDefinedVariable) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string
		Name string
		Expr string
	}{
		Type: "UserDefinedVariable",
		Name: u.Name,
		Expr: u.Expr.String(),
	})

}

//VariableName implements the SetOp interface method.
func (u *UserDefinedVariable) VariableName() string {
	return u.Name
}

//Execute implements the SetOp interface method.
func (u *UserDefinedVariable) Execute(vcursor VCursor, env evalengine.ExpressionEnv) error {
	value, err := u.Expr.Evaluate(env)
	if err != nil {
		return err
	}
	return vcursor.Session().SetUDV(u.Name, value.Value())
}

var _ SetOp = (*SysVarIgnore)(nil)

//MarshalJSON provides the type to SetOp for plan json
func (svi *SysVarIgnore) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string
		SysVarIgnore
	}{
		Type:         "SysVarIgnore",
		SysVarIgnore: *svi,
	})

}

//VariableName implements the SetOp interface method.
func (svi *SysVarIgnore) VariableName() string {
	return svi.Name
}

//Execute implements the SetOp interface method.
func (svi *SysVarIgnore) Execute(vcursor VCursor, _ evalengine.ExpressionEnv) error {
	vcursor.Session().RecordWarning(&querypb.QueryWarning{Code: mysql.ERNotSupportedYet, Message: fmt.Sprintf("Ignored inapplicable SET %v = %v", svi.Name, svi.Expr)})
	return nil
}

var _ SetOp = (*SysVarCheckAndIgnore)(nil)

//MarshalJSON provides the type to SetOp for plan json
func (svci *SysVarCheckAndIgnore) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string
		SysVarCheckAndIgnore
	}{
		Type:                 "SysVarCheckAndIgnore",
		SysVarCheckAndIgnore: *svci,
	})

}

//VariableName implements the SetOp interface method
func (svci *SysVarCheckAndIgnore) VariableName() string {
	return svci.Name
}

//Execute implements the SetOp interface method
func (svci *SysVarCheckAndIgnore) Execute(vcursor VCursor, env evalengine.ExpressionEnv) error {
	rss, _, err := vcursor.ResolveDestinations(svci.Keyspace.Name, nil, []key.Destination{svci.TargetDestination})
	if err != nil {
		return vterrors.Wrap(err, "SysVarCheckAndIgnore")
	}

	if len(rss) != 1 {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Unexpected error, DestinationKeyspaceID mapping to multiple shards: %v", svci.TargetDestination)
	}
	checkSysVarQuery := fmt.Sprintf("select 1 from dual where @@%s = %s", svci.Name, svci.Expr)
	result, err := execShard(vcursor, checkSysVarQuery, env.BindVars, rss[0], false /* rollbackOnError */, false /* canAutocommit */)
	if err != nil {
		return err
	}
	var warning *querypb.QueryWarning
	if result.RowsAffected == 0 {
		warning = &querypb.QueryWarning{Code: mysql.ERNotSupportedYet, Message: fmt.Sprintf("Modification not allowed using set construct for: %s", svci.Name)}
	} else {
		warning = &querypb.QueryWarning{Code: mysql.ERNotSupportedYet, Message: fmt.Sprintf("Ignored inapplicable SET %v = %v", svci.Name, svci.Expr)}
	}
	vcursor.Session().RecordWarning(warning)
	return nil
}

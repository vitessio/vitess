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
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/vt/srvtopo"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

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
		TargetDestination key.Destination `json:",omitempty"`
		Expr              string
	}

	// SysVarSet implements the SetOp interface and will write the changes variable into the session
	SysVarSet struct {
		Name              string
		Keyspace          *vindexes.Keyspace
		TargetDestination key.Destination `json:",omitempty"`
		Expr              string
	}

	// SysVarSetAware implements the SetOp interface and will write the changes variable into the session
	// The special part is that these settings change the sessions behaviour in different ways
	SysVarSetAware struct {
		Name string
		Expr evalengine.Expr
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
func (svi *SysVarIgnore) Execute(VCursor, evalengine.ExpressionEnv) error {
	log.Infof("Ignored inapplicable SET %v = %v", svi.Name, svi.Expr)
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
	if result.RowsAffected == 0 {
		log.Infof("Ignored inapplicable SET %v = %v", svci.Name, svci.Expr)
	}
	return nil
}

var _ SetOp = (*SysVarSet)(nil)

//MarshalJSON provides the type to SetOp for plan json
func (svs *SysVarSet) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string
		SysVarSet
	}{
		Type:      "SysVarSet",
		SysVarSet: *svs,
	})

}

//VariableName implements the SetOp interface method
func (svs *SysVarSet) VariableName() string {
	return svs.Name
}

//Execute implements the SetOp interface method
func (svs *SysVarSet) Execute(vcursor VCursor, env evalengine.ExpressionEnv) error {
	// For those running on advanced vitess settings.
	if svs.TargetDestination != nil {
		rss, _, err := vcursor.ResolveDestinations(svs.Keyspace.Name, nil, []key.Destination{svs.TargetDestination})
		if err != nil {
			return vterrors.Wrap(err, "SysVarSet")
		}
		vcursor.Session().NeedsReservedConn()
		return svs.execSetStatement(vcursor, rss, env)
	}
	isSysVarModified, err := svs.checkAndUpdateSysVar(vcursor, env)
	if err != nil {
		return err
	}
	if !isSysVarModified {
		// setting ignored, same as underlying datastore
		return nil
	}
	// Update existing shard session with new system variable settings.
	rss := vcursor.Session().ShardSession()
	if len(rss) == 0 {
		return nil
	}
	queries := make([]*querypb.BoundQuery, len(rss))
	for i := 0; i < len(rss); i++ {
		queries[i] = &querypb.BoundQuery{
			Sql:           fmt.Sprintf("set @@%s = %s", svs.Name, svs.Expr),
			BindVariables: env.BindVars,
		}
	}
	_, errs := vcursor.ExecuteMultiShard(rss, queries, false /* rollbackOnError */, false /* canAutocommit */)
	return vterrors.Aggregate(errs)
}

func (svs *SysVarSet) execSetStatement(vcursor VCursor, rss []*srvtopo.ResolvedShard, env evalengine.ExpressionEnv) error {
	queries := make([]*querypb.BoundQuery, len(rss))
	for i := 0; i < len(rss); i++ {
		queries[i] = &querypb.BoundQuery{
			Sql:           fmt.Sprintf("set @@%s = %s", svs.Name, svs.Expr),
			BindVariables: env.BindVars,
		}
	}
	_, errs := vcursor.ExecuteMultiShard(rss, queries, false /* rollbackOnError */, false /* canAutocommit */)
	return vterrors.Aggregate(errs)
}

func (svs *SysVarSet) checkAndUpdateSysVar(vcursor VCursor, res evalengine.ExpressionEnv) (bool, error) {
	sysVarExprValidationQuery := fmt.Sprintf("select %s from dual where @@%s != %s", svs.Expr, svs.Name, svs.Expr)
	rss, _, err := vcursor.ResolveDestinations(svs.Keyspace.Name, nil, []key.Destination{key.DestinationKeyspaceID{0}})
	if err != nil {
		return false, vterrors.Wrap(err, "SysVarSet")
	}
	qr, err := execShard(vcursor, sysVarExprValidationQuery, res.BindVars, rss[0], false /* rollbackOnError */, false /* canAutocommit */)
	if err != nil {
		return false, err
	}
	if len(qr.Rows) == 0 {
		return false, nil
	}
	// TODO : validate how value needs to be stored.
	value := qr.Rows[0][0]
	buf := new(bytes.Buffer)
	value.EncodeSQL(buf)
	vcursor.Session().SetSysVar(svs.Name, buf.String())
	vcursor.Session().NeedsReservedConn()
	return true, nil
}

var _ SetOp = (*SysVarSetAware)(nil)

// System variables that needs special handling
const (
	Autocommit          = "autocommit"
	ClientFoundRows     = "client_found_rows"
	SkipQueryPlanCache  = "skip_query_plan_cache"
	TxReadOnly          = "tx_read_only"
	TransactionReadOnly = "transaction_read_only"
	SQLSelectLimit      = "sql_select_limit"
	TransactionMode     = "transaction_mode"
	Workload            = "workload"
	Charset             = "charset"
	Names               = "names"
)

//MarshalJSON provides the type to SetOp for plan json
func (svss *SysVarSetAware) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string
		Name string
		Expr string
	}{
		Type: "SysVarAware",
		Name: svss.Name,
		Expr: svss.Expr.String(),
	})
}

//Execute implements the SetOp interface method
func (svss *SysVarSetAware) Execute(vcursor VCursor, env evalengine.ExpressionEnv) error {
	switch svss.Name {
	// These are all the boolean values we need to handle
	case Autocommit, ClientFoundRows, SkipQueryPlanCache, TxReadOnly, TransactionReadOnly:
		value, err := svss.Expr.Evaluate(env)
		if err != nil {
			return err
		}
		boolValue, err := value.ToBooleanStrict()
		if err != nil {
			return vterrors.Wrapf(err, "System setting '%s' can't be set to this value", svss.Name)
		}
		switch svss.Name {
		case Autocommit:
			vcursor.Session().SetAutocommit(boolValue)
		case ClientFoundRows:
			vcursor.Session().SetClientFoundRows(boolValue)
		case SkipQueryPlanCache:
			vcursor.Session().SetSkipQueryPlanCache(boolValue)
		case TxReadOnly, TransactionReadOnly:
			// TODO (4127): This is a dangerous NOP.
		}

	case SQLSelectLimit:
		value, err := svss.Expr.Evaluate(env)
		if err != nil {
			return err
		}

		v := value.Value()
		if !v.IsIntegral() {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected value type for sql_select_limit: %T", value.Value().Type().String())
		}
		intValue, err := v.ToInt64()
		if err != nil {
			return err
		}
		vcursor.Session().SetSQLSelectLimit(intValue)

		// String settings
	case TransactionMode, Workload, Charset, Names:
		value, err := svss.Expr.Evaluate(env)
		if err != nil {
			return err
		}
		v := value.Value()
		if !v.IsText() && !v.IsBinary() {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected value type for %s: %s", svss.Name, value.Value().Type().String())
		}

		str := v.ToString()
		switch svss.Name {
		case TransactionMode:
			out, ok := vtgatepb.TransactionMode_value[strings.ToUpper(str)]
			if !ok {
				return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid transaction_mode: %s", str)
			}
			vcursor.Session().SetTransactionMode(vtgatepb.TransactionMode(out))
		case Workload:
			out, ok := querypb.ExecuteOptions_Workload_value[strings.ToUpper(str)]
			if !ok {
				return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid workload: %s", str)
			}
			vcursor.Session().SetWorkload(querypb.ExecuteOptions_Workload(out))
		case Charset, Names:
			switch strings.ToLower(str) {
			case "", "utf8", "utf8mb4", "latin1", "default":
				// do nothing
				break
			default:
				return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected value for charset/names: %v", str)
			}
		}

	default:
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unsupported construct")
	}

	return nil
}

//VariableName implements the SetOp interface method
func (svss *SysVarSetAware) VariableName() string {
	return svss.Name
}

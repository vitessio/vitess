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

	"vitess.io/vitess/go/vt/sysvars"

	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/vt/srvtopo"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/schema"
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

	// SysVarReservedConn implements the SetOp interface and will write the changes variable into the session
	SysVarReservedConn struct {
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
		return err
	}

	if len(rss) != 1 {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Unexpected error, DestinationKeyspaceID mapping to multiple shards: %v", svci.TargetDestination)
	}
	checkSysVarQuery := fmt.Sprintf("select 1 from dual where @@%s = %s", svci.Name, svci.Expr)
	result, err := execShard(vcursor, checkSysVarQuery, env.BindVars, rss[0], false /* rollbackOnError */, false /* canAutocommit */)
	if err != nil {
		return err
	}
	if len(result.Rows) == 0 {
		log.Infof("Ignored inapplicable SET %v = %v", svci.Name, svci.Expr)
	}
	return nil
}

var _ SetOp = (*SysVarReservedConn)(nil)

//MarshalJSON provides the type to SetOp for plan json
func (svs *SysVarReservedConn) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string
		SysVarReservedConn
	}{
		Type:               "SysVarSet",
		SysVarReservedConn: *svs,
	})

}

//VariableName implements the SetOp interface method
func (svs *SysVarReservedConn) VariableName() string {
	return svs.Name
}

//Execute implements the SetOp interface method
func (svs *SysVarReservedConn) Execute(vcursor VCursor, env evalengine.ExpressionEnv) error {
	// For those running on advanced vitess settings.
	if svs.TargetDestination != nil {
		rss, _, err := vcursor.ResolveDestinations(svs.Keyspace.Name, nil, []key.Destination{svs.TargetDestination})
		if err != nil {
			return err
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

func (svs *SysVarReservedConn) execSetStatement(vcursor VCursor, rss []*srvtopo.ResolvedShard, env evalengine.ExpressionEnv) error {
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

func (svs *SysVarReservedConn) checkAndUpdateSysVar(vcursor VCursor, res evalengine.ExpressionEnv) (bool, error) {
	sysVarExprValidationQuery := fmt.Sprintf("select %s from dual where @@%s != %s", svs.Expr, svs.Name, svs.Expr)
	rss, _, err := vcursor.ResolveDestinations(svs.Keyspace.Name, nil, []key.Destination{key.DestinationKeyspaceID{0}})
	if err != nil {
		return false, err
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

//MarshalJSON marshals all the json
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
	var err error
	switch svss.Name {
	case sysvars.Autocommit.Name:
		err = svss.setBoolSysVar(env, vcursor.Session().SetAutocommit)
	case sysvars.ClientFoundRows.Name:
		err = svss.setBoolSysVar(env, vcursor.Session().SetClientFoundRows)
	case sysvars.SkipQueryPlanCache.Name:
		err = svss.setBoolSysVar(env, vcursor.Session().SetSkipQueryPlanCache)
	case sysvars.TxReadOnly.Name,
		sysvars.TransactionReadOnly.Name:
		// TODO (4127): This is a dangerous NOP.
		noop := func(bool) error { return nil }
		err = svss.setBoolSysVar(env, noop)
	case sysvars.SQLSelectLimit.Name:
		intValue, err := svss.evalAsInt64(env)
		if err != nil {
			return err
		}
		vcursor.Session().SetSQLSelectLimit(intValue)
	case sysvars.TransactionMode.Name:
		str, err := svss.evalAsString(env)
		if err != nil {
			return err
		}
		out, ok := vtgatepb.TransactionMode_value[strings.ToUpper(str)]
		if !ok {
			return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValueForVar, "invalid transaction_mode: %s", str)
		}
		vcursor.Session().SetTransactionMode(vtgatepb.TransactionMode(out))
	case sysvars.Workload.Name:
		str, err := svss.evalAsString(env)
		if err != nil {
			return err
		}
		out, ok := querypb.ExecuteOptions_Workload_value[strings.ToUpper(str)]
		if !ok {
			return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValueForVar, "invalid workload: %s", str)
		}
		vcursor.Session().SetWorkload(querypb.ExecuteOptions_Workload(out))
	case sysvars.DDLStrategy.Name:
		str, err := svss.evalAsString(env)
		if err != nil {
			return err
		}
		if _, _, err := schema.ParseDDLStrategy(str); err != nil {
			return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValueForVar, "invalid DDL strategy: %s", str)
		}
		vcursor.Session().SetDDLStrategy(str)
	case sysvars.SessionEnableSystemSettings.Name:
		err = svss.setBoolSysVar(env, vcursor.Session().SetSessionEnableSystemSettings)
	case sysvars.Charset.Name, sysvars.Names.Name:
		str, err := svss.evalAsString(env)
		if err != nil {
			return err
		}
		switch strings.ToLower(str) {
		case "", "utf8", "utf8mb4", "latin1", "default":
			// do nothing
			break
		default:
			return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValueForVar, "unexpected value for charset/names: %v", str)
		}
	case sysvars.ReadAfterWriteGTID.Name:
		str, err := svss.evalAsString(env)
		if err != nil {
			return err
		}
		vcursor.Session().SetReadAfterWriteGTID(str)
	case sysvars.ReadAfterWriteTimeOut.Name:
		val, err := svss.evalAsFloat(env)
		if err != nil {
			return err
		}
		vcursor.Session().SetReadAfterWriteTimeout(val)
	case sysvars.SessionTrackGTIDs.Name:
		str, err := svss.evalAsString(env)
		if err != nil {
			return err
		}
		switch strings.ToLower(str) {
		case "off":
			vcursor.Session().SetSessionTrackGTIDs(false)
		case "own_gtid":
			vcursor.Session().SetSessionTrackGTIDs(true)
		default:
			return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValueForVar, "Variable 'session_track_gtids' can't be set to the value of '%s'", str)
		}
	default:
		return vterrors.NewErrorf(vtrpcpb.Code_NOT_FOUND, vterrors.UnknownSystemVariable, "Unknown system variable '%s'", svss.Name)
	}

	return err
}

func (svss *SysVarSetAware) evalAsInt64(env evalengine.ExpressionEnv) (int64, error) {
	value, err := svss.Expr.Evaluate(env)
	if err != nil {
		return 0, err
	}

	v := value.Value()
	if !v.IsIntegral() {
		return 0, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongTypeForVar, "Incorrect argument type to variable '%s': %s", svss.Name, value.Value().Type().String())
	}
	intValue, err := v.ToInt64()
	if err != nil {
		return 0, err
	}
	return intValue, nil
}

func (svss *SysVarSetAware) evalAsFloat(env evalengine.ExpressionEnv) (float64, error) {
	value, err := svss.Expr.Evaluate(env)
	if err != nil {
		return 0, err
	}

	v := value.Value()
	floatValue, err := v.ToFloat64()
	if err != nil {
		return 0, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongTypeForVar, "Incorrect argument type to variable '%s': %s", svss.Name, value.Value().Type().String())
	}
	return floatValue, nil
}

func (svss *SysVarSetAware) evalAsString(env evalengine.ExpressionEnv) (string, error) {
	value, err := svss.Expr.Evaluate(env)
	if err != nil {
		return "", err
	}
	v := value.Value()
	if !v.IsText() && !v.IsBinary() {
		return "", vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongTypeForVar, "Incorrect argument type to variable '%s': %s", svss.Name, value.Value().Type().String())
	}

	return v.ToString(), nil
}

func (svss *SysVarSetAware) setBoolSysVar(env evalengine.ExpressionEnv, setter func(bool) error) error {
	value, err := svss.Expr.Evaluate(env)
	if err != nil {
		return err
	}
	boolValue, err := value.ToBooleanStrict()
	if err != nil {
		return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValueForVar, "Variable '%s' can't be set to the value: %s", svss.Name, err.Error())
	}
	return setter(boolValue)
}

//VariableName implements the SetOp interface method
func (svss *SysVarSetAware) VariableName() string {
	return svss.Name
}

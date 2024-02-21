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
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
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
		noTxNeeded

		Ops   []SetOp
		Input Primitive
	}

	// SetOp is an interface that different type of set operations implements.
	SetOp interface {
		Execute(ctx context.Context, vcursor VCursor, env *evalengine.ExpressionEnv) error
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
		SupportSetVar     bool
	}

	// SysVarSetAware implements the SetOp interface and will write the changes variable into the session
	// The special part is that these settings change the sessions behaviour in different ways
	SysVarSetAware struct {
		Name string
		Expr evalengine.Expr
	}

	// VitessMetadata implements the SetOp interface and will write the changes variable into the topo server
	VitessMetadata struct {
		Name, Value string
	}
)

var unsupportedSQLModes = []string{"ANSI_QUOTES", "NO_BACKSLASH_ESCAPES", "PIPES_AS_CONCAT", "REAL_AS_FLOAT"}

var _ Primitive = (*Set)(nil)

// RouteType implements the Primitive interface method.
func (s *Set) RouteType() string {
	return "Set"
}

// GetKeyspaceName implements the Primitive interface method.
func (s *Set) GetKeyspaceName() string {
	return ""
}

// GetTableName implements the Primitive interface method.
func (s *Set) GetTableName() string {
	return ""
}

// TryExecute implements the Primitive interface method.
func (s *Set) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	input, err := vcursor.ExecutePrimitive(ctx, s.Input, bindVars, false)
	if err != nil {
		return nil, err
	}
	if len(input.Rows) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "should get a single row")
	}
	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	env.Row = input.Rows[0]
	for _, setOp := range s.Ops {
		err := setOp.Execute(ctx, vcursor, env)
		if err != nil {
			return nil, err
		}
	}
	return &sqltypes.Result{}, nil
}

// TryStreamExecute implements the Primitive interface method.
func (s *Set) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	result, err := s.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(result)
}

// GetFields implements the Primitive interface method.
func (s *Set) GetFields(context.Context, VCursor, map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return &sqltypes.Result{}, nil
}

// Inputs implements the Primitive interface
func (s *Set) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{s.Input}, nil
}

func (s *Set) description() PrimitiveDescription {
	other := map[string]any{
		"Ops": s.Ops,
	}
	return PrimitiveDescription{
		OperatorType: "Set",
		Other:        other,
	}
}

var _ SetOp = (*UserDefinedVariable)(nil)

// MarshalJSON provides the type to SetOp for plan json
func (u *UserDefinedVariable) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string
		Name string
		Expr string
	}{
		Type: "UserDefinedVariable",
		Name: u.Name,
		Expr: sqlparser.String(u.Expr),
	})
}

// VariableName implements the SetOp interface method.
func (u *UserDefinedVariable) VariableName() string {
	return u.Name
}

// Execute implements the SetOp interface method.
func (u *UserDefinedVariable) Execute(ctx context.Context, vcursor VCursor, env *evalengine.ExpressionEnv) error {
	value, err := env.Evaluate(u.Expr)
	if err != nil {
		return err
	}
	return vcursor.Session().SetUDV(u.Name, value.Value(vcursor.ConnCollation()))
}

var _ SetOp = (*SysVarIgnore)(nil)

// MarshalJSON provides the type to SetOp for plan json
func (svi *SysVarIgnore) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string
		SysVarIgnore
	}{
		Type:         "SysVarIgnore",
		SysVarIgnore: *svi,
	})
}

// VariableName implements the SetOp interface method.
func (svi *SysVarIgnore) VariableName() string {
	return svi.Name
}

// Execute implements the SetOp interface method.
func (svi *SysVarIgnore) Execute(context.Context, VCursor, *evalengine.ExpressionEnv) error {
	return nil
}

var _ SetOp = (*SysVarCheckAndIgnore)(nil)

// MarshalJSON provides the type to SetOp for plan json
func (svci *SysVarCheckAndIgnore) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string
		SysVarCheckAndIgnore
	}{
		Type:                 "SysVarCheckAndIgnore",
		SysVarCheckAndIgnore: *svci,
	})
}

// VariableName implements the SetOp interface method
func (svci *SysVarCheckAndIgnore) VariableName() string {
	return svci.Name
}

// Execute implements the SetOp interface method
func (svci *SysVarCheckAndIgnore) Execute(ctx context.Context, vcursor VCursor, env *evalengine.ExpressionEnv) error {
	rss, _, err := vcursor.ResolveDestinations(ctx, svci.Keyspace.Name, nil, []key.Destination{svci.TargetDestination})
	if err != nil {
		return err
	}

	if len(rss) != 1 {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Unexpected error, DestinationKeyspaceID mapping to multiple shards: %v", svci.TargetDestination)
	}
	checkSysVarQuery := fmt.Sprintf("select 1 from dual where @@%s = %s", svci.Name, svci.Expr)
	_, err = execShard(ctx, nil, vcursor, checkSysVarQuery, env.BindVars, rss[0], false /* rollbackOnError */, false /* canAutocommit */)
	if err != nil {
		// Rather than returning the error, we will just log the error
		// as the intention for executing the query it to validate the current setting and eventually ignore it anyways.
		// There is no benefit of returning the error back to client.
		log.Warningf("unable to validate the current settings for '%s': %s", svci.Name, err.Error())
		return nil
	}
	return nil
}

var _ SetOp = (*SysVarReservedConn)(nil)

// MarshalJSON provides the type to SetOp for plan json
func (svs *SysVarReservedConn) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string
		SysVarReservedConn
	}{
		Type:               "SysVarSet",
		SysVarReservedConn: *svs,
	})
}

// VariableName implements the SetOp interface method
func (svs *SysVarReservedConn) VariableName() string {
	return svs.Name
}

// Execute implements the SetOp interface method
func (svs *SysVarReservedConn) Execute(ctx context.Context, vcursor VCursor, env *evalengine.ExpressionEnv) error {
	// For those running on advanced vitess settings.
	if svs.TargetDestination != nil {
		rss, _, err := vcursor.ResolveDestinations(ctx, svs.Keyspace.Name, nil, []key.Destination{svs.TargetDestination})
		if err != nil {
			return err
		}
		vcursor.Session().NeedsReservedConn()
		return svs.execSetStatement(ctx, vcursor, rss, env)
	}
	needReservedConn, err := svs.checkAndUpdateSysVar(ctx, vcursor, env)
	if err != nil {
		return err
	}
	if !needReservedConn {
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
			Sql:           fmt.Sprintf("set %s = %s", svs.Name, svs.Expr),
			BindVariables: env.BindVars,
		}
	}
	_, errs := vcursor.ExecuteMultiShard(ctx, nil, rss, queries, false /* rollbackOnError */, false /* canAutocommit */)
	return vterrors.Aggregate(errs)
}

func (svs *SysVarReservedConn) execSetStatement(ctx context.Context, vcursor VCursor, rss []*srvtopo.ResolvedShard, env *evalengine.ExpressionEnv) error {
	queries := make([]*querypb.BoundQuery, len(rss))
	for i := 0; i < len(rss); i++ {
		queries[i] = &querypb.BoundQuery{
			Sql:           fmt.Sprintf("set @@%s = %s", svs.Name, svs.Expr),
			BindVariables: env.BindVars,
		}
	}
	_, errs := vcursor.ExecuteMultiShard(ctx, nil, rss, queries, false /* rollbackOnError */, false /* canAutocommit */)
	return vterrors.Aggregate(errs)
}

func (svs *SysVarReservedConn) checkAndUpdateSysVar(ctx context.Context, vcursor VCursor, res *evalengine.ExpressionEnv) (bool, error) {
	sysVarExprValidationQuery := fmt.Sprintf("select %s from dual where @@%s != %s", svs.Expr, svs.Name, svs.Expr)
	if svs.Name == "sql_mode" {
		sysVarExprValidationQuery = fmt.Sprintf("select @@%s orig, %s new", svs.Name, svs.Expr)
	}
	rss, _, err := vcursor.ResolveDestinations(ctx, svs.Keyspace.Name, nil, []key.Destination{key.DestinationKeyspaceID{0}})
	if err != nil {
		return false, err
	}
	qr, err := execShard(ctx, nil, vcursor, sysVarExprValidationQuery, res.BindVars, rss[0], false /* rollbackOnError */, false /* canAutocommit */)
	if err != nil {
		return false, err
	}
	changed := len(qr.Rows) > 0
	if !changed {
		return false, nil
	}

	var value sqltypes.Value
	if svs.Name == "sql_mode" {
		changed, value, err = sqlModeChangedValue(qr)
		if err != nil {
			return false, err
		}
		if !changed {
			return false, nil
		}
	} else {
		value = qr.Rows[0][0]
	}
	var buf strings.Builder
	value.EncodeSQL(&buf)
	s := buf.String()
	vcursor.Session().SetSysVar(svs.Name, s)

	// If the condition below is true, we want to use reserved connection instead of SET_VAR query hint.
	// MySQL supports SET_VAR only in MySQL80 and for a limited set of system variables.
	if !svs.SupportSetVar || s == "''" || !vcursor.CanUseSetVar() {
		vcursor.Session().NeedsReservedConn()
		return true, nil
	}
	return false, nil
}

func sqlModeChangedValue(qr *sqltypes.Result) (bool, sqltypes.Value, error) {
	if len(qr.Fields) != 2 {
		return false, sqltypes.Value{}, nil
	}
	if len(qr.Rows[0]) != 2 {
		return false, sqltypes.Value{}, nil
	}
	orig := qr.Rows[0][0].ToString()
	newVal := qr.Rows[0][1].ToString()

	origArr := strings.Split(orig, ",")
	// Keep track of if the value is seen or not.
	origMap := map[string]bool{}
	for _, oVal := range origArr {
		// Default is not seen.
		origMap[strings.ToUpper(oVal)] = true
	}
	uniqOrigVal := len(origMap)
	origValSeen := 0

	changed := false
	newValArr := strings.Split(newVal, ",")
	unsupportedMode := ""
	for _, nVal := range newValArr {
		nVal = strings.ToUpper(nVal)
		for _, mode := range unsupportedSQLModes {
			if mode == nVal {
				unsupportedMode = nVal
				break
			}
		}
		notSeen, exists := origMap[nVal]
		if !exists {
			changed = true
			break
		}
		if exists && notSeen {
			// Value seen. Turn it off
			origMap[nVal] = false
			origValSeen++
		}
	}
	if !changed && uniqOrigVal != origValSeen {
		changed = true
	}
	if changed && unsupportedMode != "" {
		return false, sqltypes.Value{}, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "setting the %s sql_mode is unsupported", unsupportedMode)
	}

	return changed, qr.Rows[0][1], nil
}

var _ SetOp = (*SysVarSetAware)(nil)

// MarshalJSON marshals all the json
func (svss *SysVarSetAware) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string
		Name string
		Expr string
	}{
		Type: "SysVarAware",
		Name: svss.Name,
		Expr: sqlparser.String(svss.Expr),
	})
}

// Execute implements the SetOp interface method
func (svss *SysVarSetAware) Execute(ctx context.Context, vcursor VCursor, env *evalengine.ExpressionEnv) error {
	var err error
	switch svss.Name {
	case sysvars.Autocommit.Name:
		err = svss.setBoolSysVar(ctx, env, vcursor.Session().SetAutocommit)
	case sysvars.ClientFoundRows.Name:
		err = svss.setBoolSysVar(ctx, env, vcursor.Session().SetClientFoundRows)
	case sysvars.SkipQueryPlanCache.Name:
		err = svss.setBoolSysVar(ctx, env, vcursor.Session().SetSkipQueryPlanCache)
	case sysvars.TxReadOnly.Name,
		sysvars.TransactionReadOnly.Name:
		// TODO (4127): This is a dangerous NOP.
		noop := func(context.Context, bool) error { return nil }
		err = svss.setBoolSysVar(ctx, env, noop)
	case sysvars.SQLSelectLimit.Name:
		intValue, err := svss.evalAsInt64(env, vcursor)
		if err != nil {
			return err
		}
		vcursor.Session().SetSQLSelectLimit(intValue) // nolint:errcheck
	case sysvars.TransactionMode.Name:
		str, err := svss.evalAsString(env, vcursor)
		if err != nil {
			return err
		}
		out, ok := vtgatepb.TransactionMode_value[strings.ToUpper(str)]
		if !ok {
			return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValueForVar, "invalid transaction_mode: %s", str)
		}
		vcursor.Session().SetTransactionMode(vtgatepb.TransactionMode(out))
	case sysvars.Workload.Name:
		str, err := svss.evalAsString(env, vcursor)
		if err != nil {
			return err
		}
		out, ok := querypb.ExecuteOptions_Workload_value[strings.ToUpper(str)]
		if !ok {
			return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValueForVar, "invalid workload: %s", str)
		}
		vcursor.Session().SetWorkload(querypb.ExecuteOptions_Workload(out))
	case sysvars.DDLStrategy.Name:
		str, err := svss.evalAsString(env, vcursor)
		if err != nil {
			return err
		}
		if _, err := schema.ParseDDLStrategy(str); err != nil {
			return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValueForVar, "invalid DDL strategy: %s", str)
		}
		vcursor.Session().SetDDLStrategy(str)
	case sysvars.MigrationContext.Name:
		str, err := svss.evalAsString(env, vcursor)
		if err != nil {
			return err
		}
		if err := schema.ValidateMigrationContext(str); err != nil {
			return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValueForVar, "invalid migration_context: %s", str)
		}
		vcursor.Session().SetMigrationContext(str)
	case sysvars.QueryTimeout.Name:
		queryTimeout, err := svss.evalAsInt64(env, vcursor)
		if err != nil {
			return err
		}
		vcursor.Session().SetQueryTimeout(queryTimeout)
	case sysvars.SessionEnableSystemSettings.Name:
		err = svss.setBoolSysVar(ctx, env, vcursor.Session().SetSessionEnableSystemSettings)
	case sysvars.Charset.Name, sysvars.Names.Name:
		str, err := svss.evalAsString(env, vcursor)
		if err != nil {
			return err
		}
		switch strings.ToLower(str) {
		case "", "utf8", "utf8mb4", "latin1", "default":
			// do nothing
			break
		default:
			return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "charset/name %v is not supported", str)
		}
	case sysvars.ReadAfterWriteGTID.Name:
		str, err := svss.evalAsString(env, vcursor)
		if err != nil {
			return err
		}
		vcursor.Session().SetReadAfterWriteGTID(str)
	case sysvars.ReadAfterWriteTimeOut.Name:
		val, err := svss.evalAsFloat(env, vcursor)
		if err != nil {
			return err
		}
		vcursor.Session().SetReadAfterWriteTimeout(val)
	case sysvars.SessionTrackGTIDs.Name:
		str, err := svss.evalAsString(env, vcursor)
		if err != nil {
			return err
		}
		switch strings.ToLower(str) {
		case "off":
			vcursor.Session().SetSessionTrackGTIDs(false)
		case "own_gtid":
			vcursor.Session().SetSessionTrackGTIDs(true)
		default:
			return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValueForVar, "variable 'session_track_gtids' can't be set to the value of '%s'", str)
		}
	default:
		return vterrors.NewErrorf(vtrpcpb.Code_NOT_FOUND, vterrors.UnknownSystemVariable, "unknown system variable '%s'", svss.Name)
	}

	return err
}

func (svss *SysVarSetAware) evalAsInt64(env *evalengine.ExpressionEnv, vcursor VCursor) (int64, error) {
	value, err := env.Evaluate(svss.Expr)
	if err != nil {
		return 0, err
	}

	v := value.Value(vcursor.ConnCollation())
	if !v.IsIntegral() {
		return 0, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongTypeForVar, "incorrect argument type to variable '%s': %s", svss.Name, v.Type().String())
	}
	intValue, err := v.ToInt64()
	if err != nil {
		return 0, err
	}
	return intValue, nil
}

func (svss *SysVarSetAware) evalAsFloat(env *evalengine.ExpressionEnv, vcursor VCursor) (float64, error) {
	value, err := env.Evaluate(svss.Expr)
	if err != nil {
		return 0, err
	}

	v := value.Value(vcursor.ConnCollation())
	floatValue, err := v.ToFloat64()
	if err != nil {
		return 0, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongTypeForVar, "incorrect argument type to variable '%s': %s", svss.Name, v.Type().String())
	}
	return floatValue, nil
}

func (svss *SysVarSetAware) evalAsString(env *evalengine.ExpressionEnv, vcursor VCursor) (string, error) {
	value, err := env.Evaluate(svss.Expr)
	if err != nil {
		return "", err
	}
	v := value.Value(vcursor.ConnCollation())
	if !v.IsText() && !v.IsBinary() {
		return "", vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongTypeForVar, "incorrect argument type to variable '%s': %s", svss.Name, v.Type().String())
	}

	return v.ToString(), nil
}

func (svss *SysVarSetAware) setBoolSysVar(ctx context.Context, env *evalengine.ExpressionEnv, setter func(context.Context, bool) error) error {
	value, err := env.Evaluate(svss.Expr)
	if err != nil {
		return err
	}
	boolValue, err := value.ToBooleanStrict()
	if err != nil {
		return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValueForVar, "variable '%s' can't be set to the value: %s", svss.Name, err.Error())
	}
	return setter(ctx, boolValue)
}

// VariableName implements the SetOp interface method
func (svss *SysVarSetAware) VariableName() string {
	return svss.Name
}

var _ SetOp = (*VitessMetadata)(nil)

func (v *VitessMetadata) Execute(ctx context.Context, vcursor VCursor, env *evalengine.ExpressionEnv) error {
	return vcursor.SetExec(ctx, v.Name, v.Value)
}

func (v *VitessMetadata) VariableName() string {
	return v.Name
}

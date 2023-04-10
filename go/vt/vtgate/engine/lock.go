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
	"fmt"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/vt/srvtopo"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ Primitive = (*Lock)(nil)

// Lock primitive will execute sql containing lock functions
type Lock struct {
	// Keyspace specifies the keyspace to send the query to.
	Keyspace *vindexes.Keyspace

	// TargetDestination specifies an explicit target destination to send the query to.
	TargetDestination key.Destination

	FieldQuery string

	LockFunctions []*LockFunc

	noInputs

	noTxNeeded
}

type LockFunc struct {
	Typ  *sqlparser.LockingFunc
	Name evalengine.Expr
}

// RouteType is part of the Primitive interface
func (l *Lock) RouteType() string {
	return "lock"
}

// GetKeyspaceName is part of the Primitive interface
func (l *Lock) GetKeyspaceName() string {
	return l.Keyspace.Name
}

// GetTableName is part of the Primitive interface
func (l *Lock) GetTableName() string {
	return "dual"
}

// TryExecute is part of the Primitive interface
func (l *Lock) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	return l.execLock(ctx, vcursor, bindVars)
}

func (l *Lock) execLock(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	rss, _, err := vcursor.ResolveDestinations(ctx, l.Keyspace.Name, nil, []key.Destination{l.TargetDestination})
	if err != nil {
		return nil, err
	}
	if len(rss) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "lock query can be routed to single shard only: %v", rss)
	}

	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	var fields []*querypb.Field
	var rrow sqltypes.Row
	for _, lf := range l.LockFunctions {
		var lName string
		if lf.Name != nil {
			er, err := env.Evaluate(lf.Name)
			if err != nil {
				return nil, err
			}
			lName = er.Value().ToString()
		}
		qr, err := lf.execLock(ctx, vcursor, bindVars, rss[0])
		if err != nil {
			return nil, err
		}
		fields = append(fields, qr.Fields...)
		lockRes := qr.Rows[0]
		rrow = append(rrow, lockRes...)

		switch lf.Typ.Type {
		case sqlparser.IsFreeLock, sqlparser.IsUsedLock:
		case sqlparser.GetLock:
			if lockRes[0].ToString() == "1" {
				vcursor.Session().AddAdvisoryLock(lName)
			}
		case sqlparser.ReleaseAllLocks:
			err = vcursor.ReleaseLock(ctx)
			if err != nil {
				return nil, err
			}
		case sqlparser.ReleaseLock:
			// TODO: do not execute if lock not taken.
			if lockRes[0].ToString() == "1" {
				vcursor.Session().RemoveAdvisoryLock(lName)
			}
			if !vcursor.Session().AnyAdvisoryLockTaken() {
				err = vcursor.ReleaseLock(ctx)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return &sqltypes.Result{
		Fields: fields,
		Rows:   []sqltypes.Row{rrow},
	}, nil
}

func (lf *LockFunc) execLock(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, rs *srvtopo.ResolvedShard) (*sqltypes.Result, error) {
	boundQuery := &querypb.BoundQuery{
		Sql:           fmt.Sprintf("select %s from dual", sqlparser.String(lf.Typ)),
		BindVariables: bindVars,
	}
	qr, err := vcursor.ExecuteLock(ctx, rs, boundQuery, lf.Typ.Type)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) != 1 && len(qr.Fields) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected rows or fields returned for the lock function: %v", lf.Typ.Type)
	}
	return qr, nil
}

// TryStreamExecute is part of the Primitive interface
func (l *Lock) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	qr, err := l.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(qr)
}

// GetFields is part of the Primitive interface
func (l *Lock) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	rss, _, err := vcursor.ResolveDestinations(ctx, l.Keyspace.Name, nil, []key.Destination{l.TargetDestination})
	if err != nil {
		return nil, err
	}
	if len(rss) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "lock query can be routed to single shard only: %v", rss)
	}
	boundQuery := []*querypb.BoundQuery{{
		Sql:           l.FieldQuery,
		BindVariables: bindVars,
	}}
	qr, errs := vcursor.ExecuteMultiShard(ctx, l, rss, boundQuery, false, true)
	if len(errs) > 0 {
		return nil, vterrors.Aggregate(errs)
	}
	return qr, nil
}

func (l *Lock) description() PrimitiveDescription {
	other := map[string]any{
		"FieldQuery": l.FieldQuery,
	}
	var lf []string
	for _, f := range l.LockFunctions {
		lf = append(lf, sqlparser.String(f.Typ))
	}
	other["lock_func"] = lf
	return PrimitiveDescription{
		OperatorType:      "Lock",
		Keyspace:          l.Keyspace,
		TargetDestination: l.TargetDestination,
		Other:             other,
	}
}

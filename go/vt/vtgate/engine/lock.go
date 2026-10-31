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
	noInputs
	noTxNeeded

	// Keyspace specifies the keyspace to send the query to.
	Keyspace *vindexes.Keyspace

	// Keyspaces lists the keyspaces designated for advisory locks, when there
	// is more than one. Each lock is then acquired in a majority of them.
	Keyspaces []*vindexes.Keyspace

	// TargetDestination specifies an explicit target destination to send the query to.
	TargetDestination key.ShardDestination

	FieldQuery string

	LockFunctions []*LockFunc
}

type LockFunc struct {
	Typ  *sqlparser.LockingFunc
	Name evalengine.Expr
}

// TryExecute is part of the Primitive interface
func (l *Lock) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	if len(l.Keyspaces) > 1 {
		return l.execMajorityLock(ctx, vcursor, bindVars)
	}
	return l.execLock(ctx, vcursor, bindVars)
}

func (l *Lock) execLock(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	rss, _, err := vcursor.ResolveDestinations(ctx, l.Keyspace.Name, nil, []key.ShardDestination{l.TargetDestination})
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
			lName = er.Value(vcursor.ConnCollation()).ToString()
		}
		qr, err := lf.execLock(ctx, vcursor, bindVars, rss[0], false)
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

func (lf *LockFunc) execLock(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, rs *srvtopo.ResolvedShard, multiKeyspace bool) (*sqltypes.Result, error) {
	boundQuery := &querypb.BoundQuery{
		Sql:           fmt.Sprintf("select %s from dual", sqlparser.String(lf.Typ)),
		BindVariables: bindVars,
	}
	qr, err := vcursor.ExecuteLock(ctx, rs, boundQuery, lf.Typ.Type, multiKeyspace)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) != 1 && len(qr.Fields) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected rows or fields returned for the lock function: %v", lf.Typ.Type)
	}
	return qr, nil
}

// execMajorityLock runs each lock function in every designated keyspace, and
// treats a lock as held when it is held in a majority of them. A client never
// sees a lock held in only some keyspaces: a failure either leaves the lock
// unheld, or releases all of the session's locks and returns an error.
func (l *Lock) execMajorityLock(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	rss := make([]*srvtopo.ResolvedShard, len(l.Keyspaces))
	resolveErrs := make([]error, len(l.Keyspaces))
	for i, ks := range l.Keyspaces {
		resolved, _, err := vcursor.ResolveDestinations(ctx, ks.Name, nil, []key.ShardDestination{l.TargetDestination})
		if err == nil && len(resolved) != 1 {
			err = vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "lock query can be routed to single shard only: %v", resolved)
		}
		if err != nil {
			resolveErrs[i] = vterrors.Wrapf(err, "failed to resolve advisory-lock keyspace %s", ks.Name)
			continue
		}
		rss[i] = resolved[0]
	}
	majority := len(l.Keyspaces)/2 + 1

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
			lName = er.Value(vcursor.ConnCollation()).ToString()
		}

		results := make([]*sqltypes.Result, len(l.Keyspaces))
		errs := make([]error, len(l.Keyspaces))
		for i := range l.Keyspaces {
			if resolveErrs[i] != nil {
				errs[i] = resolveErrs[i]
				continue
			}
			results[i], errs[i] = lf.execLock(ctx, vcursor, bindVars, rss[i], true)
		}
		firstErr := firstError(errs)

		var value sqltypes.Value
		switch lf.Typ.Type {
		case sqlparser.GetLock:
			var acquired []int
			for i, qr := range results {
				if errs[i] == nil && lockValue(qr).ToString() == "1" {
					acquired = append(acquired, i)
				}
			}
			if len(acquired) >= majority {
				value = sqltypes.NewInt64(1)
				vcursor.Session().AddAdvisoryLock(lName)
				break
			}
			// Release the copies that were acquired, so that no keyspace
			// holds part of a lock that the client doesn't have.
			release := &LockFunc{Typ: &sqlparser.LockingFunc{Type: sqlparser.ReleaseLock, Name: lf.Typ.Name}}
			for _, i := range acquired {
				if _, err := release.execLock(ctx, vcursor, bindVars, rss[i], true); err != nil {
					return nil, releaseAllLocks(ctx, vcursor, err, "failed to release a partially acquired advisory lock")
				}
			}
			if firstErr != nil {
				return nil, firstErr
			}
			value = sqltypes.NewInt64(0)
		case sqlparser.ReleaseLock:
			if firstErr != nil {
				return nil, releaseAllLocks(ctx, vcursor, firstErr, "failed to release advisory lock")
			}
			value = combineReleaseValues(results)
			if value.ToString() == "1" {
				vcursor.Session().RemoveAdvisoryLock(lName)
			}
			if !vcursor.Session().AnyAdvisoryLockTaken() {
				if err := vcursor.ReleaseLock(ctx); err != nil {
					return nil, err
				}
			}
		case sqlparser.ReleaseAllLocks:
			if firstErr != nil {
				return nil, releaseAllLocks(ctx, vcursor, firstErr, "failed to release advisory locks")
			}
			// Each lock is counted once per keyspace that holds it, so report
			// the largest count rather than the sum.
			var released int64
			for _, qr := range results {
				if n, err := lockValue(qr).ToInt64(); err == nil && n > released {
					released = n
				}
			}
			value = sqltypes.NewInt64(released)
			if err := vcursor.ReleaseLock(ctx); err != nil {
				return nil, err
			}
		case sqlparser.IsFreeLock:
			if firstErr != nil {
				return nil, firstErr
			}
			// A lock counts as used if any keyspace holds it.
			value = sqltypes.NewInt64(1)
			for _, qr := range results {
				if lockValue(qr).ToString() == "0" {
					value = sqltypes.NewInt64(0)
				}
			}
		case sqlparser.IsUsedLock:
			if firstErr != nil {
				return nil, firstErr
			}
			// A lock counts as used if any keyspace holds it.
			value = sqltypes.NULL
			for _, qr := range results {
				if v := lockValue(qr); !v.IsNull() {
					value = v
					break
				}
			}
		default:
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unexpected lock function: %v", lf.Typ.Type)
		}

		for _, qr := range results {
			if qr != nil {
				fields = append(fields, qr.Fields...)
				break
			}
		}
		rrow = append(rrow, value)
	}
	return &sqltypes.Result{
		Fields: fields,
		Rows:   []sqltypes.Row{rrow},
	}, nil
}

// releaseAllLocks releases all of the session's advisory locks after a lock
// function failed in some keyspaces, and returns an error that says so.
func releaseAllLocks(ctx context.Context, vcursor VCursor, err error, msg string) error {
	if releaseErr := vcursor.ReleaseLock(ctx); releaseErr != nil {
		return vterrors.Wrapf(err, "%s, and failed to release held locks: %v", msg, releaseErr)
	}
	return vterrors.Wrapf(err, "%s, held locks are released", msg)
}

// combineReleaseValues combines RELEASE_LOCK results from several keyspaces.
// A lock taken before a keyspace was designated is not held there, so the
// result is 1 if any keyspace released it, else 0 if any keyspace reports it
// held by someone else, else NULL.
func combineReleaseValues(results []*sqltypes.Result) sqltypes.Value {
	value := sqltypes.NULL
	for _, qr := range results {
		switch lockValue(qr).ToString() {
		case "1":
			return sqltypes.NewInt64(1)
		case "0":
			value = sqltypes.NewInt64(0)
		}
	}
	return value
}

// lockValue returns the value of a single lock function result.
func lockValue(qr *sqltypes.Result) sqltypes.Value {
	if qr == nil || len(qr.Rows) == 0 || len(qr.Rows[0]) == 0 {
		return sqltypes.NULL
	}
	return qr.Rows[0][0]
}

func firstError(errs []error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
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
	rss, _, err := vcursor.ResolveDestinations(ctx, l.Keyspace.Name, nil, []key.ShardDestination{l.TargetDestination})
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
	qr, errs := vcursor.ExecuteMultiShard(ctx, l, rss, boundQuery, false, true, false)
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
	if len(l.LockFunctions) > 0 {
		lf = make([]string, 0, len(l.LockFunctions))
		for _, f := range l.LockFunctions {
			lf = append(lf, sqlparser.String(f.Typ))
		}
	}
	other["lock_func"] = lf
	if len(l.Keyspaces) > 1 {
		keyspaces := make([]string, 0, len(l.Keyspaces))
		for _, ks := range l.Keyspaces {
			keyspaces = append(keyspaces, ks.Name)
		}
		other["Keyspaces"] = keyspaces
	}
	return PrimitiveDescription{
		OperatorType:      "Lock",
		Keyspace:          l.Keyspace,
		TargetDestination: l.TargetDestination,
		Other:             other,
	}
}

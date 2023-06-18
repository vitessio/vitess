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

package evalengine

import (
	"context"
	"errors"
	"strings"
	"time"

	"vitess.io/vitess/go/mysql/datetime"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type VCursor interface {
	TimeZone() *time.Location
	GetKeyspace() string
}

type (
	// ExpressionEnv contains the environment that the expression
	// evaluates in, such as the current row and bindvars
	ExpressionEnv struct {
		vm vmstate

		BindVars map[string]*querypb.BindVariable
		Row      []sqltypes.Value

		// internal state
		now  time.Time
		vc   VCursor
		user *querypb.VTGateCallerID
	}
)

func (env *ExpressionEnv) time(utc bool) datetime.DateTime {
	if utc {
		return datetime.NewDateTimeFromStd(env.now.UTC())
	}
	return datetime.NewDateTimeFromStd(env.now)
}

func (env *ExpressionEnv) currentUser() string {
	if env.user == nil {
		return "vt_app@localhost"
	}
	user := env.user.GetUsername()
	if !strings.Contains(user, "@") {
		user = user + "@localhost"
	}
	return user
}

func (env *ExpressionEnv) currentDatabase() string {
	if env.vc == nil {
		return ""
	}
	return env.vc.GetKeyspace()
}

func (env *ExpressionEnv) currentTimezone() *time.Location {
	if env.vc == nil {
		return nil
	}
	return env.vc.TimeZone()
}

func (env *ExpressionEnv) Evaluate(expr Expr) (EvalResult, error) {
	if p, ok := expr.(*CompiledExpr); ok {
		return env.EvaluateVM(p)
	}
	e, err := expr.eval(env)
	return EvalResult{e}, err
}

var ErrAmbiguousType = errors.New("the type of this expression cannot be statically computed")

func (env *ExpressionEnv) TypeOf(expr Expr, fields []*querypb.Field) (sqltypes.Type, error) {
	ty, f := expr.typeof(env, fields)
	if f&flagAmbiguousType != 0 {
		return ty, ErrAmbiguousType
	}
	return ty, nil
}

// EmptyExpressionEnv returns a new ExpressionEnv with no bind vars or row
func EmptyExpressionEnv() *ExpressionEnv {
	return NewExpressionEnv(context.Background(), nil, nil)
}

// NewExpressionEnv returns an expression environment with no current row, but with bindvars
func NewExpressionEnv(ctx context.Context, bindVars map[string]*querypb.BindVariable, vc VCursor) *ExpressionEnv {
	env := &ExpressionEnv{BindVars: bindVars, vc: vc}
	env.user = callerid.ImmediateCallerIDFromContext(ctx)

	// The current time for this ExpressionEnv is set only once, during creation.
	// This is to ensure that all expressions in the same ExpressionEnv evaluate NOW()
	// and similar SQL functions to the same value.
	env.now = time.Now()

	if tz := env.currentTimezone(); tz != nil {
		env.now = env.now.In(tz)
	}
	return env
}

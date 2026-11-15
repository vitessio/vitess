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
	"strings"
	"time"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/mysql/config"
	"vitess.io/vitess/go/mysql/datetime"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtenv"
)

type VCursor interface {
	TimeZone() *time.Location
	GetKeyspace() string
	SQLMode() string
	Environment() *vtenv.Environment
	SetLastInsertID(id uint64)
}

type (
	// ExpressionEnv contains the environment that the expression
	// evaluates in, such as the current row and bindvars
	ExpressionEnv struct {
		vm vmstate

		BindVars map[string]*querypb.BindVariable
		Row      []sqltypes.Value
		Fields   []*querypb.Field

		// FieldCharsets says that the text of each column of Row is in the
		// character set of the collation that Fields has for it, and that a
		// column that is evaluated in a collation of another character set
		// has to be converted to it first, see columnText.
		FieldCharsets bool

		// internal state
		now          time.Time
		vc           VCursor
		user         *querypb.VTGateCallerID
		sqlmode      SQLMode
		collationEnv *collations.Environment
	}
)

// columnText returns raw, the text of the column at offset of the row, in the
// character set of the collation that the column is evaluated in.
//
// MySQL evaluates a column in the collation of the column, and sends its text
// in character_set_results. A tablet that reads with a utf8mb4 connection
// sends 'Müller' of a latin1_german2_ci column as 4D C3 BC 6C 6C 65 72, and
// the planner types the column as latin1_german2_ci. Evaluated as they are,
// those bytes are the latin1 text 'MÃ¼ller', which is not equal to 'Mueller'
// as 'Müller' is in that collation. So the text becomes 4D FC 6C 6C 65 72
// first. A character that the character set of the collation does not have
// becomes "?", as it does in MySQL.
//
// Only an evaluation with FieldCharsets converts. The text of a row without
// fields, and of a field with the binary collation, is taken for text of the
// collation, as it is without FieldCharsets.
func (env *ExpressionEnv) columnText(offset int, raw []byte, collation collations.ID) []byte {
	if !env.FieldCharsets || offset >= len(env.Fields) {
		return raw
	}
	to, from, ok := colldata.TextConversion(collation, collations.ID(env.Fields[offset].Charset))
	if !ok {
		return raw
	}
	// The error says that a character became "?".
	converted, _ := charset.Convert(nil, to, raw, from)
	return converted
}

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
	return env.vc.GetKeyspace()
}

func (env *ExpressionEnv) currentVersion() string {
	return env.vc.Environment().MySQLVersion()
}

func (env *ExpressionEnv) currentTimezone() *time.Location {
	return env.vc.TimeZone()
}

func (env *ExpressionEnv) Evaluate(expr Expr) (EvalResult, error) {
	if p, ok := expr.(*CompiledExpr); ok {
		return env.EvaluateVM(p)
	}
	e, err := expr.eval(env)
	return EvalResult{v: e, collationEnv: env.collationEnv}, err
}

func (env *ExpressionEnv) EvaluateAST(expr Expr) (EvalResult, error) {
	e, err := expr.eval(env)
	return EvalResult{v: e, collationEnv: env.collationEnv}, err
}

func (env *ExpressionEnv) TypeOf(expr Expr) (Type, error) {
	ty, err := expr.typeof(env)
	if err != nil {
		return Type{}, err
	}
	return NewTypeEx(ty.Type, ty.Col.Collation, ty.Flag&flagNullable != 0, ty.Size, ty.Scale, ty.Values), nil
}

func (env *ExpressionEnv) SetTime(now time.Time) {
	// This function is called only once by NewExpressionEnv to ensure that all expressions in the same
	// ExpressionEnv evaluate NOW() and similar SQL functions to the same value.
	env.now = now
	if tz := env.currentTimezone(); tz != nil {
		env.now = env.now.In(tz)
	}
}

func (env *ExpressionEnv) VCursor() VCursor {
	return env.vc
}

type emptyVCursor struct {
	env *vtenv.Environment
	tz  *time.Location
}

func (e *emptyVCursor) Environment() *vtenv.Environment {
	return e.env
}

func (e *emptyVCursor) TimeZone() *time.Location {
	return e.tz
}

func (e *emptyVCursor) GetKeyspace() string {
	return ""
}

func (e *emptyVCursor) SQLMode() string {
	return config.DefaultSQLMode
}
func (e *emptyVCursor) SetLastInsertID(_ uint64) {}

func NewEmptyVCursor(env *vtenv.Environment, tz *time.Location) VCursor {
	return &emptyVCursor{env: env, tz: tz}
}

// EmptyExpressionEnv returns a new ExpressionEnv with no bind vars or row
func EmptyExpressionEnv(env *vtenv.Environment) *ExpressionEnv {
	return NewExpressionEnv(context.Background(), nil, NewEmptyVCursor(env, time.Local))
}

// NewExpressionEnv returns an expression environment with no current row, but with bindvars
func NewExpressionEnv(ctx context.Context, bindVars map[string]*querypb.BindVariable, vc VCursor) *ExpressionEnv {
	env := &ExpressionEnv{BindVars: bindVars, vc: vc}
	env.user = callerid.ImmediateCallerIDFromContext(ctx)
	env.SetTime(time.Now())
	env.sqlmode = ParseSQLMode(vc.SQLMode())
	env.collationEnv = vc.Environment().CollationEnv()
	return env
}

const (
	sqlModeParsed = 1 << iota
	sqlModeNoZeroDate
)

type SQLMode uint32

func (mode SQLMode) AllowZeroDate() bool {
	if mode == 0 {
		// default: do not allow zero-date if the sqlmode is not set
		return false
	}
	return (mode & sqlModeNoZeroDate) == 0
}

func ParseSQLMode(sqlmode string) SQLMode {
	var mode SQLMode
	if strings.Contains(sqlmode, "NO_ZERO_DATE") {
		mode |= sqlModeNoZeroDate
	}
	mode |= sqlModeParsed
	return mode
}

/*
Copyright 2026 The Vitess Authors.

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

package planbuilder

import (
	"vitess.io/vitess/go/mysql/sqlmode"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/sysvars"
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// sql_mode reaches the vttablet on three entry points: connection settings, SET
// statements, and SET_VAR optimizer hints. Settings and SET statements validate
// constant values with MySQL's semantics (see sqlmode.Validate), returning the same
// errors the vtgate returns. NO_BACKSLASH_ESCAPES is rejected (see
// sqlmode.ValidateNoUnforwardableModes): under that mode MySQL would lex the
// vttablet-serialized text differently than it was written, so it cannot be applied
// to the MySQL session — and the vttablet answers @@sql_mode reads from that session,
// so it rejects a mode it would have to leave out of the applied value. Every other
// mode is applied as written, parse-relevant modes included: the vttablet parses
// queries under them itself, and MySQL enforces its resolution- and execution-time
// semantics. A non-constant value cannot be judged at plan time and is handled by
// entry point: connection settings reject it, since they are applied with no
// verification afterwards; a SET statement that assigns other variables alongside it
// is rejected too, since MySQL applies none of a failing SET's assignments and the
// others would already be applied by the time the value could be judged; a SET
// statement whose sole assignment it is runs under MySQL's own validation, and the
// executor reads back the applied value and judges it the same way (see
// Plan.ReadBackSQLMode). SET_VAR hints are not judged at all: a hint applies to the
// hinted statement's execution only and cannot change how that statement's own text
// is lexed, so it is forwarded for MySQL to judge — MySQL warns about and ignores
// invalid hint values.

// ValidateReservedSettings judges the settings a true reservation executes directly on
// its tainted connection — the path that does not go through BuildSettingQuery — and
// returns the parse-relevant sql_mode bits they put the session in, so the reserved
// connection can parse later queries under them. setsSQLMode reports whether the
// settings assign sql_mode at all: settings that do not leave the connection's session
// in whatever mode it already is, which the caller must keep rather than reset. It
// mirrors BuildSettingQuery's sql_mode validation: every setting must parse as a SET
// statement carrying constant sql_mode values, because the settings are applied with no
// verification afterwards and a value that cannot be judged upfront could put the MySQL
// session in a mode it must not run under.
func ValidateReservedSettings(settings []string, parser *sqlparser.Parser) (parseMode sqlparser.SQLMode, setsSQLMode bool, err error) {
	for _, setting := range settings {
		stmt, err := parser.Parse(setting)
		if err != nil {
			return 0, false, vterrors.Wrapf(err, "failed to parse connection setting: %s", setting)
		}
		set, ok := stmt.(*sqlparser.Set)
		if !ok {
			return 0, false, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "connection setting is not a SET statement: %s", setting)
		}
		if err := validateConstantSetExprsSQLMode(set.Exprs); err != nil {
			return 0, false, err
		}
		if mode, sawConstant := constantSetExprsSQLModeBits(set.Exprs); sawConstant {
			parseMode = mode
			setsSQLMode = true
		}
	}
	return parseMode, setsSQLMode, nil
}

// validateConstantSetExprsSQLMode is validateSetExprsSQLMode for the settings paths,
// which have no read-back phase: a session-scope sql_mode assignment whose value is not
// a constant cannot be judged there at all and is rejected.
func validateConstantSetExprsSQLMode(exprs sqlparser.SetExprs) error {
	readBack, err := validateSetExprsSQLMode(exprs)
	if err != nil {
		return err
	}
	if readBack {
		return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "non-constant sql_mode value in connection settings: %s", sqlparser.String(&sqlparser.Set{Exprs: exprs}))
	}
	return nil
}

// constantSetExprsSQLModeBits extracts the parse-relevant sql_mode bits of the last
// session-scope sql_mode assignment — the modes the vttablet parses queries on this
// connection under — when that assignment is a constant. sawConstant reports whether
// it is, so callers can record the session's bits even when they are zero. An earlier
// assignment in the same statement is superseded by the last one, whatever its form.
// Values the caller's validation pass did not reject as invalid are ignored.
func constantSetExprsSQLModeBits(exprs sqlparser.SetExprs) (parseMode sqlparser.SQLMode, sawConstant bool) {
	for _, expr := range exprs {
		if !isSessionSQLModeAssignment(expr) {
			continue
		}
		parseMode, sawConstant = 0, false
		mode, ok := constantSQLModeValue(expr)
		if !ok {
			continue
		}
		if mode, err := sqlmode.Validate(mode); err == nil {
			parseMode = sqlparser.ParseSQLMode(mode.String())
			sawConstant = true
		}
	}
	return parseMode, sawConstant
}

// isSessionSQLModeAssignment reports whether the assignment sets the session's
// sql_mode. The global scope is the operator's domain, not a vtgate session's.
func isSessionSQLModeAssignment(expr *sqlparser.SetExpr) bool {
	if expr.Var.Name.Lowered() != sysvars.SQLMode.Name {
		return false
	}
	switch expr.Var.Scope {
	case sqlparser.SessionScope, sqlparser.NoScope, sqlparser.NextTxScope:
		return true
	default:
		return false
	}
}

// constantSQLModeValue returns the assigned value when it is a constant.
func constantSQLModeValue(expr *sqlparser.SetExpr) (sqltypes.Value, bool) {
	lit, ok := expr.Expr.(*sqlparser.Literal)
	if !ok {
		return sqltypes.Value{}, false
	}
	value, err := sqlparser.LiteralToValue(lit)
	if err != nil {
		return sqltypes.Value{}, false
	}
	return value, true
}

// validateSetStatementSQLMode is validateSetExprsSQLMode for SET statements executed on
// a dedicated connection, where a non-constant sql_mode value is read back and judged
// after the statement ran — but only when sql_mode is the statement's sole assignment.
// The statement can still fail at that point, MySQL applies none of a SET's assignments
// when one of them fails, and a multi-assignment statement would already have applied
// its other assignments by then, so it is rejected upfront instead.
func validateSetStatementSQLMode(set *sqlparser.Set) (readBack bool, err error) {
	readBack, err = validateSetExprsSQLMode(set.Exprs)
	if err != nil {
		return false, err
	}
	if readBack && len(set.Exprs) > 1 {
		return false, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "non-constant sql_mode value in a multi-assignment SET: %s", sqlparser.String(set))
	}
	return readBack, nil
}

// validateSetExprsSQLMode judges the session-scope sql_mode assignments of a statement
// the way MySQL applies them: in order, each constant value validated as MySQL would
// (sqlmode.Validate), with the last assignment deciding the mode the session ends up
// in. Only that final value is held to the modes the MySQL session must not run under
// (sqlmode.ValidateNoUnforwardableModes): an earlier assignment in the same statement
// is superseded before the connection processes anything else. When the last
// assignment is not a constant it cannot be judged here; readBack=true then asks the
// executor to read back the applied value after the statement runs and judge it then.
func validateSetExprsSQLMode(exprs sqlparser.SetExprs) (readBack bool, err error) {
	var last sqlmode.Mode
	var lastConstant, seen bool
	for _, expr := range exprs {
		if !isSessionSQLModeAssignment(expr) {
			continue
		}
		seen = true
		value, ok := constantSQLModeValue(expr)
		if !ok {
			lastConstant = false
			continue
		}
		mode, err := sqlmode.Validate(value)
		if err != nil {
			return false, err
		}
		last, lastConstant = mode, true
	}
	if !seen {
		return false, nil
	}
	if !lastConstant {
		return true, nil
	}
	if err := sqlmode.ValidateNoUnforwardableModes(last); err != nil {
		return false, err
	}
	return false, nil
}

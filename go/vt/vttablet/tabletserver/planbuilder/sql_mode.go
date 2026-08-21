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
// semantics. Non-constant expressions cannot be judged at plan time; MySQL validates
// them itself and the executor reads back what was applied. SET_VAR hints are not
// judged at all: a hint applies to the hinted statement's execution only and cannot
// change how that statement's own text is lexed, so it is forwarded for MySQL to
// judge — MySQL warns about and ignores invalid hint values.

// ValidateReservedSettings judges the settings a true reservation executes directly on
// its tainted connection — the path that does not go through BuildSettingQuery — and
// returns the parse-relevant sql_mode bits they put the session in, so the reserved
// connection can parse later queries under them. It mirrors BuildSettingQuery's
// sql_mode validation: every setting must parse as a SET statement carrying constant
// sql_mode values, because the settings are applied with no verification afterwards
// and a value that cannot be judged upfront could put the MySQL session in a mode it
// must not run under.
func ValidateReservedSettings(settings []string, parser *sqlparser.Parser) (sqlparser.SQLMode, error) {
	var parseMode sqlparser.SQLMode
	for _, setting := range settings {
		stmt, err := parser.Parse(setting)
		if err != nil {
			return 0, vterrors.Wrapf(err, "failed to parse connection setting: %s", setting)
		}
		set, ok := stmt.(*sqlparser.Set)
		if !ok {
			return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "connection setting is not a SET statement: %s", setting)
		}
		if err := validateConstantSetExprsSQLMode(set.Exprs); err != nil {
			return 0, err
		}
		if mode, sawConstant := constantSetExprsSQLModeBits(set.Exprs); sawConstant {
			parseMode = mode
		}
	}
	return parseMode, nil
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
// constant session-scope sql_mode assignment — the modes the vttablet parses queries on
// this connection under. sawConstant reports whether one was seen, so callers can
// record the session's bits even when they are zero. Values the caller's validation
// pass did not reject as invalid are ignored.
func constantSetExprsSQLModeBits(exprs sqlparser.SetExprs) (parseMode sqlparser.SQLMode, sawConstant bool) {
	for _, expr := range exprs {
		if expr.Var.Name.Lowered() != sysvars.SQLMode.Name {
			continue
		}
		switch expr.Var.Scope {
		case sqlparser.SessionScope, sqlparser.NoScope, sqlparser.NextTxScope:
		default:
			continue
		}
		lit, ok := expr.Expr.(*sqlparser.Literal)
		if !ok {
			continue
		}
		value, err := sqlparser.LiteralToValue(lit)
		if err != nil {
			continue
		}
		mode, err := sqlmode.Validate(value)
		if err != nil {
			continue
		}
		parseMode = sqlparser.ParseSQLMode(mode.String())
		sawConstant = true
	}
	return parseMode, sawConstant
}

// validateSetExprsSQLMode rejects session-scope sql_mode assignments whose constant
// value fails sqlmode.Validate or carries a mode the MySQL session must not run under
// (sqlmode.ValidateNoUnforwardableModes). Assignments whose value is not a constant
// cannot be judged here; for those it returns readBack=true, asking the executor to
// read back the applied value after the statement runs and judge it then.
func validateSetExprsSQLMode(exprs sqlparser.SetExprs) (readBack bool, err error) {
	for _, expr := range exprs {
		if expr.Var.Name.Lowered() != sysvars.SQLMode.Name {
			continue
		}
		switch expr.Var.Scope {
		case sqlparser.SessionScope, sqlparser.NoScope, sqlparser.NextTxScope:
		default:
			// the global scope is the operator's domain, not a vtgate session's
			continue
		}
		lit, ok := expr.Expr.(*sqlparser.Literal)
		if !ok {
			readBack = true
			continue
		}
		value, err := sqlparser.LiteralToValue(lit)
		if err != nil {
			readBack = true
			continue
		}
		mode, err := sqlmode.Validate(value)
		if err != nil {
			return false, err
		}
		if err := sqlmode.ValidateNoUnforwardableModes(mode); err != nil {
			return false, err
		}
	}
	return readBack, nil
}

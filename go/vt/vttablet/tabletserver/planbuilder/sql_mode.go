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
// errors the vtgate returns, and reject the modes that change how SQL text is read
// except the ones the parser honors (sqlparser.HonoredSQLModes): the vttablet parses
// each query itself and sends MySQL its own serialization, so it must never leave a
// MySQL session in a mode its parser does not read under. A value carrying an honored
// mode is applied as written, the connection records the mode, and every query on the
// connection is parsed under it, so the vttablet reads the text the way MySQL does.
// A non-constant value cannot be judged at plan time and is handled by entry point:
// connection settings reject it, since they are applied with no verification
// afterwards; a SET statement that assigns other variables alongside it is rejected
// too, since MySQL applies none of a failing SET's assignments and the others would
// already be applied by the time the value could be judged; a SET statement whose
// sole assignment it is runs, has its applied value read back and judged the same
// way, and is restored on failure (see Plan.VerifySQLMode). SET_VAR hints are not
// judged at all: a hint applies to the hinted statement's execution only and cannot
// change how that statement's own text is lexed, so it is forwarded for MySQL to
// judge, which warns about and ignores invalid hint values.

// ValidateReservedSettings judges the settings a true reservation executes directly on
// its tainted connection, the path that does not go through BuildSettingQuery, and
// returns the lexer modes the parser honors of the sql_mode they put the session in,
// so that the reserved connection is read under them. setsSQLMode reports whether the
// settings assign sql_mode at all: settings that do not leave the connection's session
// in whatever mode it already is, which the caller must keep rather than reset. It
// mirrors BuildSettingQuery's validation: every setting must parse as a SET statement,
// with no subquery under strict table ACL, carrying constant sql_mode values, because
// the settings are applied with no verification afterwards and a value that cannot be
// judged upfront could put the MySQL session in a mode it must not run under.
func ValidateReservedSettings(settings []string, parser *sqlparser.Parser, strictTableACL bool) (parseMode sqlmode.Mode, setsSQLMode bool, err error) {
	// each setting is read under the lexer modes of the sql_mode the settings before
	// it put the session in, the way MySQL reads them, see BuildSettingQuery
	settingParser := parser
	for _, setting := range settings {
		stmt, err := settingParser.Parse(setting)
		if err != nil {
			return 0, false, vterrors.Wrapf(err, "failed to parse connection setting: %s", setting)
		}
		set, ok := stmt.(*sqlparser.Set)
		if !ok {
			return 0, false, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "connection setting is not a SET statement: %s", setting)
		}
		if strictTableACL {
			if err := rejectSettingSubqueries(set, setting); err != nil {
				return 0, false, err
			}
		}
		if err := validateConstantSetExprsSQLMode(set.Exprs); err != nil {
			return 0, false, err
		}
		mode, sawConstant, err := constantSetExprsSQLModeBits(set.Exprs)
		if err != nil {
			return 0, false, err
		}
		if sawConstant {
			parseMode = mode
			setsSQLMode = true
			settingParser = parser.WithSQLMode(mode)
		}
	}
	return parseMode, setsSQLMode, nil
}

// rejectSettingSubqueries refuses a connection setting whose expressions embed
// a subquery. A setting is applied to the connection with no table ACL check,
// so the tables a subquery reads would go unchecked. Settings carry constants:
// vtgate evaluates a SET's expression on a shard, where the tablet checks it
// like any read, and sends the value. The check only runs under strict table
// ACL: without it there is nothing for it to protect, and a vtgate from
// before the value was sent still sends a targeted session's SET expression
// as written, which would break for nothing.
func rejectSettingSubqueries(set *sqlparser.Set, setting string) error {
	var found bool
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if _, ok := node.(*sqlparser.Subquery); ok {
			found = true
		}
		return !found, nil
	}, set)
	if found {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "connection setting must not contain a subquery: %s", setting)
	}
	return nil
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

// constantSetExprsSQLModeBits extracts, of the last session-scope sql_mode assignment,
// the lexer modes the parser honors (sqlparser.HonoredSQLModes), the modes the
// vttablet reads queries on this connection under, when that assignment is a constant.
// sawConstant reports whether it is, so callers can record the session's modes even
// when they are zero. An earlier assignment in the same statement is superseded by the
// last one, whatever its form. A constant that fails validation, or a qualified name,
// is an error, the same one validateSetExprsSQLMode returns for it.
func constantSetExprsSQLModeBits(exprs sqlparser.SetExprs) (parseMode sqlmode.Mode, sawConstant bool, err error) {
	for _, expr := range exprs {
		if !isSessionSQLModeAssignment(expr) {
			continue
		}
		parseMode, sawConstant = 0, false
		value, ok, err := constantSQLModeValue(expr.Expr)
		if err != nil {
			return 0, false, err
		}
		if !ok {
			continue
		}
		mode, err := sqlmode.Validate(value, sqlparser.HonoredSQLModes)
		if err != nil {
			return 0, false, err
		}
		parseMode = mode & sqlparser.HonoredSQLModes
		sawConstant = true
	}
	return parseMode, sawConstant, nil
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
// in order. Every constant value is validated as a SET would be (sqlmode.Validate with
// the modes the parser honors), so a rejected mode is an error wherever it appears in
// the statement, even when a later assignment would supersede it. A constant is a
// literal or an unquoted mode name: MySQL accepts `SET sql_mode = TRADITIONAL` as
// `SET sql_mode = 'TRADITIONAL'`, and the parser yields the unquoted name as a bare,
// unqualified column name. A qualified name is rejected the way MySQL rejects it (see
// constantSQLModeValue). The last assignment decides what the executor does
// afterwards: when it is not a constant it cannot be judged here, and readBack=true
// asks the executor to read back the applied value after the statement runs and judge
// it then. An earlier non-constant assignment is superseded by a later constant one
// before the connection processes anything else, so it needs no read-back.
func validateSetExprsSQLMode(exprs sqlparser.SetExprs) (readBack bool, err error) {
	for _, expr := range exprs {
		if !isSessionSQLModeAssignment(expr) {
			continue
		}
		value, ok, err := constantSQLModeValue(expr.Expr)
		if err != nil {
			return false, err
		}
		if !ok {
			readBack = true
			continue
		}
		if _, err := sqlmode.Validate(value, sqlparser.HonoredSQLModes); err != nil {
			return false, err
		}
		readBack = false
	}
	return readBack, nil
}

// constantSQLModeValue returns the value of a constant sql_mode expression: a literal, or
// an unquoted mode name, which MySQL accepts as the equivalent string. A qualified name is
// never a mode name: MySQL rejects it as the wrong argument type, whatever the qualifier,
// and so does this, with MySQL's error. Any other expression is not a constant.
func constantSQLModeValue(expr sqlparser.Expr) (value sqltypes.Value, ok bool, err error) {
	switch node := expr.(type) {
	case *sqlparser.Literal:
		value, err := sqlparser.LiteralToValue(node)
		if err != nil {
			return sqltypes.Value{}, false, nil
		}
		return value, true, nil
	case *sqlparser.ColName:
		if !node.Qualifier.IsEmpty() {
			return sqltypes.Value{}, false, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongTypeForVar, "Incorrect argument type to variable '%s'", sysvars.SQLMode.Name)
		}
		return sqltypes.NewVarChar(node.Name.String()), true, nil
	}
	return sqltypes.Value{}, false, nil
}

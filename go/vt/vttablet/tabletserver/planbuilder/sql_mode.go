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
)

// sql_mode reaches the vttablet on three entry points: connection settings, SET
// statements, and SET_VAR optimizer hints. Each of them validates constant values with
// MySQL's semantics (see sqlmode.Validate), returning the same errors the vtgate
// returns. Valid values are accepted in full — parse-relevant modes included: the
// vttablet parses queries under those modes itself and gives MySQL a value with them
// stripped, so MySQL always lexes the vttablet-generated text under the default rules.
// Non-constant expressions cannot be judged or stripped at plan time; MySQL validates
// them itself and the executor reads back what was applied.

// BuildReservedSettings prepares the settings a true reservation executes directly on
// its tainted connection — the path that does not go through BuildSettingQuery. It
// mirrors BuildSettingQuery's sql_mode validation and stripping: constant sql_mode
// assignments are rewritten with their parse-relevant modes removed, and the extracted
// bits are returned so the reserved connection can parse later queries under them.
// Strings that do not parse as SET statements are passed through untouched for MySQL to
// judge, as before.
func BuildReservedSettings(settings []string, parser *sqlparser.Parser) ([]string, sqlparser.SQLMode, error) {
	applied := make([]string, len(settings))
	var parseMode sqlparser.SQLMode
	for i, setting := range settings {
		applied[i] = setting
		stmt, err := parser.Parse(setting)
		if err != nil {
			continue
		}
		set, ok := stmt.(*sqlparser.Set)
		if !ok {
			continue
		}
		if _, err := validateSetExprsSQLMode(set.Exprs); err != nil {
			return nil, 0, err
		}
		mode, sawConstant, rewrote := stripSetExprsSQLMode(set.Exprs)
		if rewrote {
			applied[i] = sqlparser.String(set)
		}
		if sawConstant {
			parseMode = mode
		}
	}
	return applied, parseMode, nil
}

// stripSetExprsSQLMode rewrites constant session-scope sql_mode assignments in place:
// when the value carries parse-relevant modes, the literal is replaced by its canonical
// form with those modes removed — the vttablet parses SQL under them itself and sends
// mode-independent text to MySQL. parseMode holds the parse-relevant bits of the last
// constant assignment, sawConstant reports whether one was seen (so callers can record
// the session's bits even when they are zero), and rewrote reports whether any literal
// changed. Values the caller's validation pass did not reject as invalid are left
// untouched.
func stripSetExprsSQLMode(exprs sqlparser.SetExprs) (parseMode sqlparser.SQLMode, sawConstant, rewrote bool) {
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
		canonical := mode.String()
		parseMode = sqlparser.ParseSQLMode(canonical)
		sawConstant = true
		if parseMode != 0 {
			expr.Expr = sqlparser.NewStrLiteral(sqlparser.StripParseRelevantModes(canonical))
			rewrote = true
		}
	}
	return parseMode, sawConstant, rewrote
}

// validateSetExprsSQLMode rejects session-scope sql_mode assignments whose constant value
// fails sqlmode.Validate. Assignments whose value is not a constant cannot be judged here;
// for those it returns readBack=true, asking the executor to read back the applied value
// after the statement runs, to record its parse-relevant modes and strip them from the
// MySQL connection.
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
		if _, err := sqlmode.Validate(value); err != nil {
			return false, err
		}
	}
	return readBack, nil
}

// validateSetVarHintSQLMode rejects a query whose SET_VAR optimizer hint carries a
// constant sql_mode value that fails sqlmode.Validate.
func validateSetVarHintSQLMode(parser *sqlparser.Parser, comments *sqlparser.ParsedComments) error {
	valText := comments.GetMySQLSetVarValue(sysvars.SQLMode.Name)
	if valText == "" {
		return nil
	}
	expr, err := parser.ParseExpr(valText)
	if err != nil {
		// not judgeable here; MySQL warns about malformed hints and ignores them
		return nil
	}
	lit, ok := expr.(*sqlparser.Literal)
	if !ok {
		return nil
	}
	value, err := sqlparser.LiteralToValue(lit)
	if err != nil {
		return nil
	}
	_, err = sqlmode.Validate(value)
	return err
}

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

// The vtgate rejects sql_mode values that change how SQL text is interpreted (see
// sqlmode.Validate), but sql_mode can reach the vttablet without passing that check: as
// connection settings or SET statements from an older vtgate in a mixed-version cluster,
// or from clients that talk to the query service directly. The checks below mirror the
// vtgate's validation at each of those entry points, returning the same errors. Only
// constant values can be judged here — non-constant expressions pass through, and MySQL
// still applies its own validation to them.

// ValidateSettingsSQLMode mirrors BuildSettingQuery's sql_mode validation for settings
// that are applied without going through BuildSettingQuery — a true reservation executes
// its settings directly on the tainted connection. Strings that do not parse as SET
// statements are left for MySQL to judge, as before.
func ValidateSettingsSQLMode(settings []string, parser *sqlparser.Parser) error {
	for _, setting := range settings {
		stmt, err := parser.Parse(setting)
		if err != nil {
			continue
		}
		set, ok := stmt.(*sqlparser.Set)
		if !ok {
			continue
		}
		if _, err := validateSetExprsSQLMode(set.Exprs); err != nil {
			return err
		}
	}
	return nil
}

// validateSetExprsSQLMode rejects session-scope sql_mode assignments whose constant value
// fails sqlmode.Validate. Assignments whose value is not a constant cannot be judged here;
// for those it returns verify=true, asking the executor to read back and validate the
// applied value after the statement runs.
func validateSetExprsSQLMode(exprs sqlparser.SetExprs) (verify bool, err error) {
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
			verify = true
			continue
		}
		value, err := sqlparser.LiteralToValue(lit)
		if err != nil {
			verify = true
			continue
		}
		if _, err := sqlmode.Validate(value); err != nil {
			return false, err
		}
	}
	return verify, nil
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

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

// The vtgate rejects sql_mode values that change how SQL text is interpreted (see
// sqlmode.Validate), but sql_mode can reach the vttablet without passing that check: as
// connection settings or SET statements from an older vtgate in a mixed-version cluster,
// or from clients that talk to the query service directly. The checks below mirror the
// vtgate's validation at each of those entry points, returning the same errors. Only
// constant values can be judged here — non-constant expressions pass through, and MySQL
// still applies its own validation to them.
//
// SET_VAR optimizer hints are not judged: a hint applies to the hinted statement's
// execution only and cannot change how that statement's own text is lexed, which is the
// vttablet's only stake in sql_mode — the vttablet does not evaluate expressions. The
// hint is forwarded verbatim, and MySQL warns about and ignores an invalid hint value as
// it does for clients that send the hint to it directly. The hint's effect on execution
// is a vtgate concern.

// ValidateSettingsSQLMode mirrors BuildSettingQuery's sql_mode validation for settings
// that are applied without going through BuildSettingQuery — a true reservation executes
// its settings directly on the tainted connection. Like BuildSettingQuery, every setting
// must parse as a SET statement, and sql_mode values must be constants: the settings
// paths apply their statements with no verification afterwards, so a value that cannot
// be judged upfront is rejected rather than applied unchecked.
func ValidateSettingsSQLMode(settings []string, parser *sqlparser.Parser) error {
	for _, setting := range settings {
		stmt, err := parser.Parse(setting)
		if err != nil {
			return vterrors.Wrapf(err, "failed to parse connection setting: %s", setting)
		}
		set, ok := stmt.(*sqlparser.Set)
		if !ok {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "connection setting is not a SET statement: %s", setting)
		}
		if err := validateConstantSetExprsSQLMode(set.Exprs); err != nil {
			return err
		}
	}
	return nil
}

// validateConstantSetExprsSQLMode is validateSetExprsSQLMode for the settings paths,
// which have no verify-after-execute phase: a session-scope sql_mode assignment whose
// value is not a constant cannot be judged there at all and is rejected.
func validateConstantSetExprsSQLMode(exprs sqlparser.SetExprs) error {
	verify, err := validateSetExprsSQLMode(exprs)
	if err != nil {
		return err
	}
	if verify {
		return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "non-constant sql_mode value in connection settings: %s", sqlparser.String(&sqlparser.Set{Exprs: exprs}))
	}
	return nil
}

// validateSetStatementSQLMode is validateSetExprsSQLMode for SET statements executed on
// a dedicated connection, where a non-constant sql_mode value can be verified after the
// statement ran — but only when sql_mode is the statement's sole assignment. MySQL
// applies none of a SET's assignments when one of them fails; a multi-assignment
// statement whose sql_mode can only be judged afterwards would already have applied its
// other assignments by then, so it is rejected upfront instead.
func validateSetStatementSQLMode(set *sqlparser.Set) (verify bool, err error) {
	verify, err = validateSetExprsSQLMode(set.Exprs)
	if err != nil {
		return false, err
	}
	if verify && len(set.Exprs) > 1 {
		return false, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "non-constant sql_mode value in a multi-assignment SET: %s", sqlparser.String(set))
	}
	return verify, nil
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

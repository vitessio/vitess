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
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// connectionCharsetVariables are the session variables that set the character
// set a connection's statements are read in or converted with, and the
// variables that SET NAMES and SET CHARACTER SET parse into.
var connectionCharsetVariables = map[string]bool{
	"character_set_client":     true,
	"character_set_connection": true,
	"character_set_results":    true,
	"collation_connection":     true,
	"names":                    true,
	"charset":                  true,
}

// validateSetExprsCharset rejects a session-scope assignment that would switch a
// connection to a character set that Vitess cannot parse and escape safely (see
// collations.IsConnectionCharsetName). A connection only starts out in a safe
// character set: these assignments can still reach the vttablet as connection
// settings, which a VTGate gRPC client controls through its session's system
// variables, or as SET statements from clients that talk to the query service
// directly. VTGate itself never sends them, so only a constant naming a safe
// character set or collation is accepted, plus NULL for character_set_results,
// which only turns result conversion off. DEFAULT and non-constant values
// resolve to a character set that cannot be judged here, and are rejected.
func validateSetExprsCharset(exprs sqlparser.SetExprs) error {
	for _, expr := range exprs {
		name := expr.Var.Name.Lowered()
		if !connectionCharsetVariables[name] {
			continue
		}
		switch expr.Var.Scope {
		case sqlparser.SessionScope, sqlparser.NoScope, sqlparser.NextTxScope:
		default:
			// the global scope is the operator's domain, not a session's
			continue
		}
		switch value := expr.Expr.(type) {
		case *sqlparser.Literal:
			if value.Type == sqlparser.StrVal && collations.IsConnectionCharsetName(value.Val) {
				continue
			}
		case *sqlparser.ColName:
			if value.Qualifier.IsEmpty() && collations.IsConnectionCharsetName(value.Name.String()) {
				continue
			}
		case *sqlparser.NullVal:
			if name == "character_set_results" {
				continue
			}
		}
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported connection character set %s for %s: use utf8mb4", sqlparser.String(expr.Expr), name)
	}
	return nil
}

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

package operators

import (
	"io"

	"vitess.io/vitess/go/vt/sqlparser"
)

// containsNonDeterministicFunction reports whether node calls a function that MySQL can evaluate
// to a different value for each row, or on each shard, within one statement.
func containsNonDeterministicFunction(node sqlparser.SQLNode) bool {
	found := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch node := node.(type) {
		case *sqlparser.CurTimeFuncExpr:
			found = node.Name.Lowered() == "sysdate"
		case *sqlparser.FuncExpr:
			switch node.Name.Lowered() {
			case "rand", "uuid", "uuid_short", "random_bytes":
				found = true
			}
		}
		if found {
			return false, io.EOF
		}
		return true, nil
	}, node)
	return found
}

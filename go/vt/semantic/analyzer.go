/*
Copyright 2020 The Vitess Authors.

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

package semantic

import (
	"vitess.io/vitess/go/vt/sqlparser"
)

//Analyse traverses the AST and finds some errors and
//annotates all ColName's to point to the table they originate from
func Analyse(stmt sqlparser.Statement) (*scope, error) {
	scoper := &scoper{current: []*scope{{}}}
	check := &checker{}
	var err error
	down := func(cursor *sqlparser.Cursor) bool {
		if err == nil {
			current := cursor.Node()
			scope := scoper.VisitDown(current, cursor.Parent())
			err = DoBinding(scope, current)
			if err == nil {
				err = check.VisitDown(current)
			}
		}
		// we return true here so that the Up function will get called, even if we found errors
		return true
	}
	up := func(cursor *sqlparser.Cursor) bool {
		if err != nil {
			return false
		}
		node := cursor.Node()
		scoper.VisitUp(node, cursor.Parent())
		err = check.VisitUp(node)
		return true
	}
	sqlparser.Rewrite(stmt, down, up)
	if err != nil {
		return nil, err
	}
	return scoper.Result()
}

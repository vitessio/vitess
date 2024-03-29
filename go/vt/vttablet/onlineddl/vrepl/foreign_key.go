/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/
/*
Copyright 2021 The Vitess Authors.

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

package vrepl

import (
	"vitess.io/vitess/go/vt/schemadiff"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
)

// RemovedForeignKeyNames returns the names of removed foreign keys, ignoring mere name changes
func RemovedForeignKeyNames(
	venv *vtenv.Environment,
	originalCreateTable string,
	vreplCreateTable string,
) (names []string, err error) {
	if originalCreateTable == "" || vreplCreateTable == "" {
		return nil, nil
	}
	env := schemadiff.NewEnv(venv, venv.CollationEnv().DefaultConnectionCharset())
	diffHints := schemadiff.DiffHints{
		ConstraintNamesStrategy: schemadiff.ConstraintNamesIgnoreAll,
	}
	diff, err := schemadiff.DiffCreateTablesQueries(env, originalCreateTable, vreplCreateTable, &diffHints)
	if err != nil {
		return nil, err
	}

	validateWalk := func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.DropKey:
			if node.Type == sqlparser.ForeignKeyType {
				names = append(names, node.Name.String())
			}
		}
		return true, nil
	}
	_ = sqlparser.Walk(validateWalk, diff.Statement()) // We never return an error
	return names, nil
}

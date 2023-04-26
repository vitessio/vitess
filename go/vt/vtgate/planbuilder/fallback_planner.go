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

package planbuilder

import (
	"fmt"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"vitess.io/vitess/go/vt/sqlparser"
)

type fallbackPlanner struct {
	primary, fallback stmtPlanner
}

var _ stmtPlanner = (*fallbackPlanner)(nil).plan

func (fp *fallbackPlanner) safePrimary(stmt sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema) (res *planResult, err error) {
	defer func() {
		// if the primary planner panics, we want to catch it here so we can fall back
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r) // not using vterror since this will only be used for logging
		}
	}()
	res, err = fp.primary(stmt, reservedVars, vschema)
	return
}

func (fp *fallbackPlanner) plan(stmt sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema) (*planResult, error) {
	res, err := fp.safePrimary(sqlparser.CloneStatement(stmt), reservedVars, vschema)
	if err != nil {
		return fp.fallback(stmt, reservedVars, vschema)
	}
	return res, nil
}

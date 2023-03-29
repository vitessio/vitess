/*
Copyright 2023 The Vitess Authors.

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
	"regexp"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

var regexParams = regexp.MustCompile(`^v\d+`)

func buildPrepareStmtPlan(query string, pStmt *sqlparser.PrepareStmt, vschema plancontext.VSchema, enableOnlineDDL, enableDirectDDL bool) (*planResult, error) {
	stmtName := pStmt.Name.Lowered()
	vschema.ClearPrepareData(stmtName)

	var pQuery, udv string
	var udvType bool
	switch expr := pStmt.Statement.(type) {
	case *sqlparser.Literal:
		pQuery = expr.Val
	case *sqlparser.Variable:
		udv = expr.Name.Lowered()
		udvType = true
	case sqlparser.Argument:
		udv, _ = strings.CutPrefix(string(expr), sqlparser.UserDefinedVariableName)
		udvType = true
	default:
		return nil, vterrors.VT13002("prepare statement should not have :%T", pStmt.Statement)
	}
	if udvType {
		bv := vschema.GetUDV(udv)
		if bv == nil {
			return nil, vterrors.VT03024(udv)
		}
		val, err := sqltypes.BindVariableToValue(bv)
		if err != nil {
			return nil, err
		}
		pQuery = val.ToString()
	}

	stmt, bindVars, err := sqlparser.Parse2(pQuery)
	if err != nil {
		return nil, err
	}
	reservedVars := sqlparser.NewReservedVars("vtg", bindVars)
	prim, err := createInstructionFor(query, stmt, reservedVars, vschema, enableOnlineDDL, enableDirectDDL)
	if err != nil {
		return nil, err
	}

	var paramsCount int
	for key := range bindVars {
		if regexParams.MatchString(key) {
			paramsCount++
		}
	}

	return &planResult{
		primitive: &engine.PrepareStmt{
			Name:       stmtName,
			Stmt:       stmt,
			ParamCount: paramsCount,
			Input:      prim.primitive,
		},
		tables: prim.tables,
	}, nil
}

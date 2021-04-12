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

package planbuilder

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

type expressionConverter struct {
	tabletExpressions []sqlparser.Expr
}

func booleanValues(astExpr sqlparser.Expr) evalengine.Expr {
	var (
		ON  = evalengine.NewLiteralInt(1)
		OFF = evalengine.NewLiteralInt(0)
	)
	switch node := astExpr.(type) {
	case *sqlparser.Literal:
		//set autocommit = 'on'
		if node.Type == sqlparser.StrVal {
			switch strings.ToLower(node.Val) {
			case "on":
				return ON
			case "off":
				return OFF
			}
		}
	case *sqlparser.ColName:
		//set autocommit = on
		if node.Name.AtCount() == sqlparser.NoAt {
			switch node.Name.Lowered() {
			case "on":
				return ON
			case "off":
				return OFF
			}
		}
	}
	return nil
}

func identifierAsStringValue(astExpr sqlparser.Expr) evalengine.Expr {
	colName, isColName := astExpr.(*sqlparser.ColName)
	if isColName {
		return evalengine.NewLiteralString([]byte(colName.Name.Lowered()))
	}
	return nil
}

func (ec *expressionConverter) convert(astExpr sqlparser.Expr, boolean, identifierAsString bool) (evalengine.Expr, error) {
	if boolean {
		evalExpr := booleanValues(astExpr)
		if evalExpr != nil {
			return evalExpr, nil
		}
	}
	if identifierAsString {
		evalExpr := identifierAsStringValue(astExpr)
		if evalExpr != nil {
			return evalExpr, nil
		}
	}
	evalExpr, err := sqlparser.Convert(astExpr)
	if err != nil {
		if err != sqlparser.ErrExprNotSupported {
			return nil, err
		}
		evalExpr = &evalengine.Column{Offset: len(ec.tabletExpressions)}
		ec.tabletExpressions = append(ec.tabletExpressions, astExpr)
	}
	return evalExpr, nil
}

func (ec *expressionConverter) source(vschema ContextVSchema) (engine.Primitive, error) {
	if len(ec.tabletExpressions) == 0 {
		return &engine.SingleRow{}, nil
	}
	ks, dest, err := resolveDestination(vschema)
	if err != nil {
		return nil, err
	}

	var expr []string
	for _, e := range ec.tabletExpressions {
		expr = append(expr, sqlparser.String(e))
	}
	query := fmt.Sprintf("select %s from dual", strings.Join(expr, ","))

	primitive := &engine.Send{
		Keyspace:          ks,
		TargetDestination: dest,
		Query:             query,
		IsDML:             false,
		SingleShardOnly:   true,
	}
	return primitive, nil
}

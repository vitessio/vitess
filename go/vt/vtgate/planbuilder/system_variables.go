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

	"vitess.io/vitess/go/vt/sysvars"

	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

func init() {
	forSettings(sysvars.ReadOnly, buildSetOpReadOnly)
	forSettings(sysvars.IgnoreThese, buildSetOpIgnore)
	forSettings(sysvars.UseReservedConn, buildSetOpReservedConn)
	forSettings(sysvars.CheckAndIgnore, buildSetOpCheckAndIgnore)
	forSettings(sysvars.NotSupported, buildNotSupported)
	forSettings(sysvars.VitessAware, buildSetOpVitessAware)
}

func forSettings(systemVariables []sysvars.SystemVariable, f func(setting) planFunc) {
	for _, sysvar := range systemVariables {
		if _, alreadyExists := sysVarPlanningFunc[sysvar.Name]; alreadyExists {
			panic("bug in set plan init - " + sysvar.Name + " already configured")
		}

		s := setting{
			name:               sysvar.Name,
			boolean:            sysvar.IsBoolean,
			identifierAsString: sysvar.IdentifierAsString,
		}

		if sysvar.Default != "" {
			s.defaultValue = parseAndBuildDefaultValue(sysvar)
		}
		sysVarPlanningFunc[sysvar.Name] = f(s)
	}
}

func parseAndBuildDefaultValue(sysvar sysvars.SystemVariable) evalengine.Expr {
	stmt, err := sqlparser.Parse(fmt.Sprintf("select %s", sysvar.Default))
	if err != nil {
		panic(fmt.Sprintf("bug in set plan init - default value for %s not parsable: %s", sysvar.Name, sysvar.Default))
	}
	sel := stmt.(*sqlparser.Select)
	aliasedExpr := sel.SelectExprs[0].(*sqlparser.AliasedExpr)
	def, err := sqlparser.Convert(aliasedExpr.Expr)
	if err != nil {
		panic(fmt.Sprintf("bug in set plan init - default value for %s not able to convert to evalengine.Expr: %s", sysvar.Name, sysvar.Default))
	}
	return def
}

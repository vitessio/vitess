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
	"sync"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/sysvars"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

type sysvarPlanCache struct {
	funcs map[string]planFunc
	once  sync.Once
}

func (pc *sysvarPlanCache) initForSettings(systemVariables []sysvars.SystemVariable, f func(setting) planFunc) {
	for _, sysvar := range systemVariables {
		if _, alreadyExists := pc.funcs[sysvar.Name]; alreadyExists {
			panic("bug in set plan init - " + sysvar.Name + " already configured")
		}

		s := setting{
			name:               sysvar.Name,
			boolean:            sysvar.IsBoolean,
			identifierAsString: sysvar.IdentifierAsString,
			supportSetVar:      sysvar.SupportSetVar,
			storageCase:        sysvar.Case,
		}

		if sysvar.Default != "" {
			s.defaultValue = pc.parseAndBuildDefaultValue(sysvar)
		}
		pc.funcs[sysvar.Name] = f(s)
	}
}

func (pc *sysvarPlanCache) parseAndBuildDefaultValue(sysvar sysvars.SystemVariable) evalengine.Expr {
	stmt, err := sqlparser.Parse(fmt.Sprintf("select %s", sysvar.Default))
	if err != nil {
		panic(fmt.Sprintf("bug in set plan init - default value for %s not parsable: %s", sysvar.Name, sysvar.Default))
	}
	sel := stmt.(*sqlparser.Select)
	aliasedExpr := sel.SelectExprs[0].(*sqlparser.AliasedExpr)
	def, err := evalengine.Translate(aliasedExpr.Expr, nil)
	if err != nil {
		panic(fmt.Sprintf("bug in set plan init - default value for %s not able to convert to evalengine.Expr: %s", sysvar.Name, sysvar.Default))
	}
	return def
}

func (pc *sysvarPlanCache) init() {
	pc.once.Do(func() {
		pc.funcs = make(map[string]planFunc)
		pc.initForSettings(sysvars.ReadOnly, buildSetOpReadOnly)
		pc.initForSettings(sysvars.IgnoreThese, buildSetOpIgnore)
		pc.initForSettings(sysvars.UseReservedConn, buildSetOpReservedConn)
		pc.initForSettings(sysvars.CheckAndIgnore, buildSetOpCheckAndIgnore)
		pc.initForSettings(sysvars.NotSupported, buildNotSupported)
		pc.initForSettings(sysvars.VitessAware, buildSetOpVitessAware)
	})
}

var sysvarPlanningFuncs sysvarPlanCache

func (pc *sysvarPlanCache) Get(expr *sqlparser.SetExpr) (planFunc, error) {
	pc.init()
	pf, ok := pc.funcs[expr.Var.Name.Lowered()]
	if !ok {
		return nil, vterrors.VT05006(sqlparser.String(expr))
	}
	return pf, nil
}

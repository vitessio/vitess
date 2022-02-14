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
	def, err := evalengine.Convert(aliasedExpr.Expr, nil)
	if err != nil {
		panic(fmt.Sprintf("bug in set plan init - default value for %s not able to convert to evalengine.Expr: %s", sysvar.Name, sysvar.Default))
	}
	return def
}

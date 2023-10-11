package evalengine

import (
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
)

func CoerceTo(value sqltypes.Value, typ sqltypes.Type) (sqltypes.Value, error) {
	cast, err := valueToEvalCast(value, value.Type(), collations.Unknown)
	if err != nil {
		return sqltypes.Value{}, err
	}
	return evalToSQLValueWithType(cast, typ), nil
}

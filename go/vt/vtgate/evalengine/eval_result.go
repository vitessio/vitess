package evalengine

import (
	"fmt"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/evalengine/decimal"
)

type (
	EvalResult struct {
		typ       querypb.Type
		collation collations.TypedCollation
		numval    uint64
		bytes     []byte
		tuple     *[]EvalResult
		decimal   *decimalResult
	}

	decimalResult struct {
		num  decimal.Big
		frac int
	}
)

// Value allows for retrieval of the value we expose for public consumption
func (e EvalResult) Value() sqltypes.Value {
	return e.toSQLValue(e.typ)
}

// TupleValues allows for retrieval of the value we expose for public consumption
func (e EvalResult) TupleValues() []sqltypes.Value {
	if e.tuple == nil {
		return nil
	}

	values := *e.tuple
	result := make([]sqltypes.Value, 0, len(values))
	for _, val := range values {
		result = append(result, val.Value())
	}
	return result
}

func (e EvalResult) textual() bool {
	return sqltypes.IsText(e.typ) || sqltypes.IsBinary(e.typ)
}

// debugString prints the entire EvalResult in a debug format
func (e *EvalResult) debugString() string {
	return fmt.Sprintf("(%s) 0x%08x %s", querypb.Type_name[int32(e.typ)], e.numval, e.bytes)
}

package evalengine

import (
	"fmt"
	"math"
	"strings"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
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

var (
	resultNull = EvalResult{typ: sqltypes.Null, collation: collationNull}
)

// Value allows for retrieval of the value we expose for public consumption
func (e *EvalResult) Value() sqltypes.Value {
	return e.toSQLValue(e.typ)
}

// TupleValues allows for retrieval of the value we expose for public consumption
func (e *EvalResult) TupleValues() []sqltypes.Value {
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

func (e *EvalResult) textual() bool {
	return sqltypes.IsText(e.typ) || sqltypes.IsBinary(e.typ)
}

func (e *EvalResult) null() bool {
	return e.typ == sqltypes.Null
}

// debugString prints the entire EvalResult in a debug format
func (e *EvalResult) debugString() string {
	return fmt.Sprintf("(%s) 0x%08x %s", querypb.Type_name[int32(e.typ)], e.numval, e.bytes)
}

//ToBooleanStrict is used when the casting to a boolean has to be minimally forgiving,
//such as when assigning to a system variable that is expected to be a boolean
func (e *EvalResult) ToBooleanStrict() (bool, error) {
	intToBool := func(i int) (bool, error) {
		switch i {
		case 0:
			return false, nil
		case 1:
			return true, nil
		default:
			return false, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%d is not a boolean", i)
		}
	}

	switch e.typ {
	case sqltypes.Int8, sqltypes.Int16, sqltypes.Int32, sqltypes.Int64:
		return intToBool(int(e.numval))
	case sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint32, sqltypes.Uint64:
		return intToBool(int(e.numval))
	case sqltypes.VarBinary:
		lower := strings.ToLower(string(e.bytes))
		switch lower {
		case "on":
			return true, nil
		case "off":
			return false, nil
		default:
			return false, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "'%s' is not a boolean", lower)
		}
	}
	return false, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "is not a boolean")
}

func (e *EvalResult) nonzero() boolean {
	switch e.typ {
	case sqltypes.Null:
		return boolNULL
	case sqltypes.Int8, sqltypes.Int16, sqltypes.Int32, sqltypes.Int64, sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint32, sqltypes.Uint64:
		return makeboolean(e.numval != 0)
	case sqltypes.Float64, sqltypes.Float32:
		return makeboolean(math.Float64frombits(e.numval) != 0.0)
	case sqltypes.Decimal:
		return makeboolean(!e.decimal.num.IsZero())
	case sqltypes.VarBinary:
		return makeboolean(parseStringToFloat(string(e.bytes)) != 0.0)
	case querypb.Type_TUPLE:
		panic("did not typecheck tuples")
	default:
		return boolTrue
	}
}

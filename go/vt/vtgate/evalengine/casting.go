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

package evalengine

import (
	"math"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

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

type boolean int8

const (
	boolFalse boolean = 0
	boolTrue  boolean = 1
	boolNULL  boolean = -1
)

func makeboolean(b bool) boolean {
	if b {
		return boolTrue
	}
	return boolFalse
}

func (b boolean) not() boolean {
	switch b {
	case boolFalse:
		return boolTrue
	case boolTrue:
		return boolFalse
	default:
		return b
	}
}

func (b boolean) evalResult() EvalResult {
	if b == boolNULL {
		return resultNull
	}
	return evalResultBool(b == boolTrue)
}

func (e *EvalResult) truthy() (boolean, error) {
	switch e.typ {
	case sqltypes.Null:
		return boolNULL, nil
	case sqltypes.Int8, sqltypes.Int16, sqltypes.Int32, sqltypes.Int64, sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint32, sqltypes.Uint64:
		return makeboolean(e.numval != 0), nil
	case sqltypes.Float64, sqltypes.Float32:
		return makeboolean(math.Float64frombits(e.numval) != 0.0), nil
	case sqltypes.Decimal:
		return makeboolean(!e.decimal.num.IsZero()), nil
	case sqltypes.VarBinary:
		return makeboolean(parseStringToFloat(string(e.bytes)) != 0.0), nil
	case querypb.Type_TUPLE:
		return boolFalse, cardinalityError(1)
	default:
		return boolTrue, nil
	}
}

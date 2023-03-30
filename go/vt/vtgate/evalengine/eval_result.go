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

package evalengine

import (
	"strings"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type EvalResult struct {
	v eval
}

// Value allows for retrieval of the value we expose for public consumption
func (er EvalResult) Value() sqltypes.Value {
	return evalToSQLValue(er.v)
}

func (er EvalResult) Collation() collations.ID {
	return evalCollation(er.v).Collation
}

func (er EvalResult) String() string {
	return er.Value().String()
}

// TupleValues allows for retrieval of the value we expose for public consumption
func (er EvalResult) TupleValues() []sqltypes.Value {
	switch v := er.v.(type) {
	case *evalTuple:
		result := make([]sqltypes.Value, 0, len(v.t))
		for _, val := range v.t {
			result = append(result, evalToSQLValue(val))
		}
		return result
	default:
		return nil
	}
}

func (er EvalResult) MustBoolean() bool {
	b, err := er.ToBooleanStrict()
	if err != nil {
		panic(err)
	}
	return b
}

// ToBooleanStrict is used when the casting to a boolean has to be minimally forgiving,
// such as when assigning to a system variable that is expected to be a boolean
func (er EvalResult) ToBooleanStrict() (bool, error) {
	switch v := er.v.(type) {
	case *evalInt64:
		switch v.i {
		case 0:
			return false, nil
		case 1:
			return true, nil
		default:
			return false, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%d is not a boolean", v.i)
		}
	case *evalUint64:
		switch v.u {
		case 0:
			return false, nil
		case 1:
			return true, nil
		default:
			return false, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%d is not a boolean", v.u)
		}
	case *evalBytes:
		switch strings.ToLower(v.string()) {
		case "on":
			return true, nil
		case "off":
			return false, nil
		default:
			return false, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "'%s' is not a boolean", v.string())
		}
	}
	return false, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "'%s' is not a boolean", er.v.ToRawBytes())
}

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
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type EvalResult struct {
	v eval
}

// Value allows for retrieval of the value we expose for public consumption.
// It will be converted to the passed in collation which is the connection
// collation and what the client expects the result to be in.
func (er EvalResult) Value(id collations.ID) sqltypes.Value {
	str, ok := er.v.(*evalBytes)
	if !ok || str.isBinary() || str.col.Collation == collations.Unknown || str.col.Collation == id {
		return evalToSQLValue(er.v)
	}

	dst, err := charset.Convert(nil, id.Get().Charset(), str.bytes, str.col.Collation.Get().Charset())
	if err != nil {
		// If we can't convert, we just return what we have, but it's going
		// to be invalidly encoded. Should normally never happen as only utf8mb4
		// is really supported for the connection character set anyway and all
		// other charsets can be converted to utf8mb4.
		return sqltypes.MakeTrusted(str.SQLType(), str.bytes)
	}
	return sqltypes.MakeTrusted(str.SQLType(), dst)
}

func (er EvalResult) Collation() collations.ID {
	return evalCollation(er.v).Collation
}

func (er EvalResult) String() string {
	return er.Value(collations.Default()).String()
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

func (er EvalResult) ToBoolean() bool {
	return evalIsTruthy(er.v) == boolTrue
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
